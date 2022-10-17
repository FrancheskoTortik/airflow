import logging
from datetime import timedelta
from enum import Enum
from functools import cached_property
from itertools import chain
from typing import Any, Callable, Dict, List, Literal, Optional, Set, Tuple, Union

import jinja2
import pendulum
import pydantic
from airflow_extensions.constants import LOCAL_TZ, TASK_MIN_DTTM
from airflow_extensions.exceptions import DWHError
from airflow_extensions.hooks.exasol_hook import ExaConnectionExt, ExasolHook
from airflow_extensions.operators.base_operators.base_operator import DWHBaseTaskOperator
from airflow_extensions.utils import exasol
from airflow_extensions.utils.messenger import SimpleMessage, TelegramWarningSender
from dateutil.relativedelta import relativedelta
from jinja2 import meta


class MartMethod(str, Enum):
    MERGE = 'merge'
    INCREMENT = 'increment'
    REPLACE = 'replace'
    APPEND = 'append'


class Stage(str, Enum):
    CREATE_TEMP = 'create_temp'
    TRUNCATE_TARGET = 'truncate_target'
    INSERT_TEMP_TO_TARGET = 'insert_temp_to_target'
    DROP_TEMP = 'drop_temp'
    DELETE_INCREMENT_TARGET = 'delete_increment_target'
    MERGE_TEMP_TO_TARGET = 'merge_temp_to_target'


# pylint: disable=no-member
class MartConfig(pydantic.BaseModel):
    exa_schema: str = pydantic.Field(alias='schema')
    table: str
    method: MartMethod
    min_date: pendulum.Date = TASK_MIN_DTTM
    lower_bound_start_of: Literal['day', 'week', 'month', 'year', 'decade', 'century'] = 'day'
    report_dt_column: Optional[str]
    lag: Union[timedelta, relativedelta] = timedelta(days=0)

    class Config:
        frozen = True
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)

        if self.method is MartMethod.INCREMENT:
            assert self.report_dt_column, f'{self.table}: increment requires "report_dt_column" param'


# pylint: disable=no-member
class StageContext(pydantic.BaseModel):
    _template: jinja2.Template = pydantic.PrivateAttr()
    _tmp_tables: Dict[str, str] = pydantic.PrivateAttr()

    exa_schema: str = pydantic.Field(alias='schema')
    table: str
    lower_bound: pendulum.Date
    upper_bound: pendulum.Date
    is_initial: bool
    report_dt_column: Optional[str]
    debug: bool

    class Config:
        frozen = True
        json_encoders = {pendulum.DateTime: lambda v: v.to_date_string()}
        keep_untouched = (cached_property,)

    def __init__(self, template: jinja2.Template, tmp_tables: List[str] = None, **data):
        super().__init__(**data)
        self._template = template

        if tmp_tables is None:
            tmp_tables = []

        self._tmp_tables = {
            tmp_table_name: f'etl.{self.exa_schema}__{self.table}__{tmp_table_name}' for tmp_table_name in tmp_tables
        }

    @property
    def target(self):
        return f'{self.exa_schema}.{self.table}'

    @property
    def tmp_tables(self):
        return self._tmp_tables

    @property
    def tmp_final(self):
        if len(self.tmp_tables) == 0:
            return None

        if len(self.tmp_tables) == 1:
            tmp, *_ = self.tmp_tables.values()
            return tmp

        return self.tmp_tables[MartOperator.final_tmp_table]

    @cached_property
    def jinja_context(self) -> jinja2.runtime.Context:
        return self._template.new_context(
            {
                'target': self.target,
                **self.tmp_tables,
                **self.dict(by_alias=True),
            }
        )


SubstageCallable = Callable[[ExaConnectionExt, StageContext], None]
PreparedStage = Tuple[Optional[str], SubstageCallable]


def truncate_target(conn: ExaConnectionExt, context: StageContext):
    temp_schema, temp_table = context.tmp_final.split('.')

    if exasol.is_table_empty(conn, temp_schema, temp_table):
        warning_message = SimpleMessage()
        message = f'<!subteam^SSQU5UUDV> при расчете витрины {context.target} был сформирован пустой инкремент.'
        TelegramWarningSender(warning_message).send(message)
        return
    conn.execute(f'truncate table {context.target};').close()


def insert_temp_to_target(conn: ExaConnectionExt, context: StageContext):
    sys_columns = {
        'PROCESSED_DTTM',
    }

    tmp_final = context.tmp_final
    if tmp_final is None:
        raise DWHError(f'{context.target} missing tmp table to insert from!')

    with conn.execute(f'describe {context.target};') as target_columns_stmt:
        target_columns = set(target_columns_stmt.fetchcol())

    with conn.execute(f'describe {context.tmp_final};') as tmp_columns_stmt:
        tmp_columns = set(tmp_columns_stmt.fetchcol())

    missing_target_columns = target_columns - tmp_columns
    if missing_target_columns - sys_columns:
        raise DWHError(f'{tmp_final} missing columns {missing_target_columns} to insert into target')

    columns = ', '.join(tmp_columns & target_columns)
    conn.execute(f'insert into {context.target} ({columns}) select {columns} from {tmp_final};').close()


def drop_temp(conn: ExaConnectionExt, context: StageContext):
    if context.debug:
        logging.info(f'Debug is ON, tmp tables {context.tmp_tables} not dropped.')
        return

    for tmp in context.tmp_tables.values():
        conn.execute(f'drop table if exists {tmp};').close()


def delete_increment_target(conn: ExaConnectionExt, context: StageContext):
    """
    Удаляет инкрементальный период из целевой таблицы по колонке report_dt_column за интервал:
    [max(target_interval_lower_bound, increment_min_dt), target_interval_upper_bound)
    """
    temp_schema, temp_table = context.tmp_final.split('.')

    if exasol.is_table_empty(conn, temp_schema, temp_table):
        warning_message = SimpleMessage()
        message = f'<!subteam^SSQU5UUDV> при расчете витрины {context.target} был сформирован пустой инкремент.'
        TelegramWarningSender(warning_message).send(message)
        return

    increment_min_dt_raw = exasol.get_column_min(conn, temp_schema, temp_table, context.report_dt_column)
    if increment_min_dt_raw is None:
        lower_bound = context.lower_bound
    else:
        increment_min_dt = pendulum.parse(increment_min_dt_raw)
        if isinstance(increment_min_dt, pendulum.DateTime):
            increment_min_dt = increment_min_dt.date()

        lower_bound = max(increment_min_dt, context.lower_bound)
        if increment_min_dt > context.lower_bound:
            warning_message = SimpleMessage()
            message = (
                f'<!subteam^SSQU5UUDV> при расчете витрины {context.target} минимальная дата в'
                f'TMP таблице больше нижней границы инкремента.'
            )
            TelegramWarningSender(warning_message).send(message)

    conn.execute(
        f'delete from {context.target}'
        f' where {context.report_dt_column} >= \'{lower_bound.to_date_string()}\''
        f' and {context.report_dt_column} < \'{context.upper_bound.to_date_string()}\';'
    ).close()


class MartOperator(DWHBaseTaskOperator):
    """
    Оператор для сбора витрин. Поддерживает методы, описанные в method_stages.
    Методы разбиты на последовательные стадии - Stage - для которых можно реализовать
    дефолтное поведение (default_stages).
    При инициализации оператор загружает шаблон и, согласно методу в конфиге,
    разбивает его на стадии, используя в случае необходимости дефолтное поведение
    для стадий (если в шаблоне этап отсутствует).
    Определение стадий осуществляется на основе имен блоков в шаблоне.
    """

    method_stages = {
        MartMethod.MERGE: [
            Stage.CREATE_TEMP,
            Stage.MERGE_TEMP_TO_TARGET,
            Stage.DROP_TEMP,
        ],
        MartMethod.INCREMENT: [
            Stage.CREATE_TEMP,
            Stage.DELETE_INCREMENT_TARGET,
            Stage.INSERT_TEMP_TO_TARGET,
            Stage.DROP_TEMP,
        ],
        MartMethod.REPLACE: [
            Stage.CREATE_TEMP,
            Stage.TRUNCATE_TARGET,
            Stage.INSERT_TEMP_TO_TARGET,
            Stage.DROP_TEMP,
        ],
        MartMethod.APPEND: [
            Stage.CREATE_TEMP,
            Stage.INSERT_TEMP_TO_TARGET,
            Stage.DROP_TEMP,
        ],
    }
    default_stages: Dict[Stage, SubstageCallable] = {
        Stage.TRUNCATE_TARGET: truncate_target,
        Stage.INSERT_TEMP_TO_TARGET: insert_temp_to_target,
        Stage.DROP_TEMP: drop_temp,
        Stage.DELETE_INCREMENT_TARGET: delete_increment_target,
    }
    commit_after: Set[Stage] = {
        Stage.CREATE_TEMP,
    }
    final_tmp_table = 'tmp_final'

    def __init__(self, config: MartConfig, template_dir: str, exasol_conn_id: str = 'exasol', **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.exasol_conn_id = exasol_conn_id

        jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir))
        template_name = f'{self.config.table}.sql.jinja2'
        self.template = jinja_env.get_template(template_name)
        self.stages = self.prepare_stages(template_name)

        self.tmp_tables = [
            variable for variable in self.get_template_variables(jinja_env, template_name) if variable.startswith('tmp')
        ]
        if len(self.tmp_tables) > 1 and self.final_tmp_table not in self.tmp_tables:
            raise DWHError(f'{template_name}: missing "{self.final_tmp_table}" temporary table')

    def __deepcopy__(self, *args, **kwargs):
        """
        FIXME DWH-2259
        Временно очищает self.template для того, чтобы скопировать оператор,
        так как copy/deepcopy не умеют работать с jinja2.Template.
        template в копии будет тем же самым инстансом, что и в оригинале.
        """
        tmp_template, self.template = self.template, None
        result = super().__deepcopy__(*args, **kwargs)
        self.template = result.template = tmp_template
        return result

    def prepare_stages(self, template_name: str) -> List[Tuple[Stage, List[PreparedStage]]]:
        """
        Собирает из шаблона и default_stages функции, которые будут вызваны в execute

        :param template_name: наименование файла конфига для сообщений об ошибках
        :return: подготовленные к запуску стадии
        """
        parsed_substages = [
            self.template_block_to_substage(template_name, block_name.lower(), block)
            for block_name, block in self.template.blocks.items()
        ]
        stages = [
            (
                stage,
                list(
                    chain(
                        (substage_name, substage_callable)
                        for block_stage, substage_name, substage_callable in parsed_substages
                        if block_stage == stage
                    )
                )
                or ([(None, self.default_stages[stage])] if stage in self.default_stages else None),
            )
            for stage in self.method_stages[self.config.method]
        ]
        missing_stages = [stage for stage, substages in stages if substages is None]
        if len(missing_stages) > 0:
            raise DWHError(f'{template_name}: missing blocks for stages "{", ".join(missing_stages)}"')

        return stages

    def template_block_to_substage(
        self, template_name: str, block_name: str, block: Callable
    ) -> Tuple[Stage, Optional[str], SubstageCallable]:
        """
        :param template_name: имя шаблона для вывода ошибок
        :param block_name: имя блока из шаблона
        :param block: jinja блок
        :return: описание подстадии в кортеже
        """
        allowed_stages = self.method_stages[self.config.method]
        allowed_stage_names = '/'.join(allowed_stages)
        substage_name: Optional[str]
        try:
            stage_name, substage_name = block_name.lower().split('__', 1)
        except ValueError:
            stage_name = block_name.lower()
            substage_name = None
        try:
            stage = Stage(stage_name)
        except ValueError as exc:
            raise DWHError(
                f'{template_name}: invalid stage "{stage_name}" not matching any of {allowed_stage_names}'
            ) from exc

        if stage not in allowed_stages:
            raise DWHError(
                f'{template_name}: stage "{stage_name}" is not allowed for method'
                f' "{self.config.method}" (allowed: {allowed_stage_names})'
            )
        substage_callable = self.block_runner_factory(template_name, block_name, block)

        return stage, substage_name, substage_callable

    @staticmethod
    def block_runner_factory(template_name: str, block_name: str, block: Callable):
        """
        :param template_name: имя шаблона для логирования
        :param block_name: имя блока
        :param block: jinja блок
        :return: функция, выполняющая sql блок из шаблона в exasol
        """

        def block_runner(conn: ExaConnectionExt, context: StageContext):
            sql = ''.join(block(context.jinja_context))
            if sql.strip():
                conn.execute(sql).close()
            else:
                logging.warning(f'{template_name}: {block_name} is empty')

        return block_runner

    @staticmethod
    def get_template_variables(jinja_env: jinja2.Environment, template_name: str) -> Set[str]:
        raw_template = jinja_env.loader.get_source(jinja_env, template_name)
        template_ast = jinja_env.parse(raw_template)
        return meta.find_undeclared_variables(template_ast)

    def execute(self, context: Any):
        hook = ExasolHook(exasol_conn_id=self.exasol_conn_id)
        data_interval_start: pendulum.DateTime = context['data_interval_start'].in_tz(LOCAL_TZ)

        with hook.get_conn() as conn:
            is_initial = exasol.is_table_empty(conn, self.config.exa_schema, self.config.table)
            lower_bound, upper_bound = self.get_date_bounds(conn, is_initial, data_interval_start.date())
            context = StageContext(
                self.template,
                self.tmp_tables,
                schema=self.config.exa_schema,
                table=self.config.table,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                is_initial=is_initial,
                report_dt_column=self.config.report_dt_column,
                debug=context.get('debug', False),
            )

            conn.commit()
            for stage, substages in self.stages:
                logging.info(f'Running stage {stage.name.lower()}')
                for substage_name, substage_callable in substages:
                    if substage_name is not None:
                        logging.info(f'Running substage {substage_name}')

                    substage_callable(conn, context)
                    if stage in self.commit_after:
                        conn.commit()
            conn.commit()

    def get_date_bounds(
        self, conn: ExaConnectionExt, is_initial: bool, data_interval_start: pendulum.Date
    ) -> Tuple[pendulum.Date, pendulum.Date]:
        if is_initial:
            lower_bound: pendulum.Date = self.config.min_date
        else:
            max_table_date = self.get_max_dt_column_date(conn)
            interval_start = data_interval_start - self.config.lag
            if max_table_date is not None:
                lower_bound = min(interval_start, max_table_date)
            else:
                lower_bound = interval_start

        return lower_bound.start_of(self.config.lower_bound_start_of), data_interval_start + timedelta(days=1)

    def get_max_dt_column_date(self, conn: ExaConnectionExt) -> Optional[pendulum.Date]:
        if not self.config.report_dt_column:
            return None

        raw_max_date = exasol.get_column_max(
            conn, self.config.exa_schema, self.config.table, self.config.report_dt_column
        )
        if raw_max_date is None:
            return None

        max_date = pendulum.parse(raw_max_date)
        if isinstance(max_date, pendulum.DateTime):
            max_date = max_date.date()
        if not isinstance(max_date, pendulum.Date):
            logging.warning(f'Got unexpected max_date: {max_date}')
            return None

        return max_date
