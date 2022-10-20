from datetime import timedelta

import jinja2
import pendulum
import pytest
from airflow_extensions.exceptions import DWHError
from airflow_extensions.operators.mart_operator import MartConfig, MartMethod, MartOperator, Stage, StageContext
from pytest_mock import MockerFixture


def mock_stage_context(template):
    lower = pendulum.DateTime(2020, 1, 1).to_datetime_string()
    upper = pendulum.DateTime(2020, 1, 2).to_datetime_string()
    return StageContext(
        template,
        schema='test_schema',
        table='test_table',
        lower_bound=lower,
        upper_bound=upper,
        report_dt_column='world',
        debug=False,
    )


def operator_factory(mocker: MockerFixture, method: MartMethod, template: str = None, **config_params) -> MartOperator:
    """
    Function for create MartOperator for test's
    Args:
        mocker - example MockerFixture
        method - example MartMethod
        template - jinja template
    Returns:
        example MartOperator
    """
    original_parse = jinja2.Environment().parse
    mock_jinja = mocker.patch('airflow_extensions.operators.mart_operator.jinja2.Environment')
    if template is not None:
        mock_jinja().get_template.return_value = jinja2.Template(template)
        mock_jinja().loader.get_source.return_value = template
        mock_jinja().parse = original_parse
    else:
        mocker.patch('airflow_extensions.operators.mart_operator.MartOperator.prepare_stages')
    operator = MartOperator(
        task_id='test_task',
        template_dir='fake template dir',
        params={'discreteness': 60},
        config=MartConfig(schema='test_schema', table='test_table', method=method, **config_params),
    )
    return operator


def test_block_runner_factory__regular_block__renders_block_and_executes(mocker: MockerFixture):
    template = jinja2.Template(
        '''
        {%- block test -%}
            hello, {{ report_dt_column }}
            from {{ lower_bound }} to {{ upper_bound }}
        {%- endblock -%}
        '''
    )
    mock_conn = mocker.MagicMock()
    context = mock_stage_context(template)

    runnable_stage = MartOperator.block_runner_factory('test_template', 'test_block', template.blocks['test'])
    runnable_stage(mock_conn, context)
    mock_conn.execute.assert_called_once_with(
        'hello, world\n            from 2020-01-01 00:00:00 to 2020-01-02 00:00:00'
    )


def test_block_runner_factory__empty_block__nothing_executed(mocker: MockerFixture):
    template = jinja2.Template('{%- block test -%} {%- endblock -%}')
    mock_conn = mocker.MagicMock()
    context = mock_stage_context(template)

    runnable_stage = MartOperator.block_runner_factory('test_template', 'test_block', template.blocks['test'])
    runnable_stage(mock_conn, context)
    mock_conn.execute.assert_not_called()


def test_get_template_variables__single_tmp_table__correct_list_returned(mocker: MockerFixture):
    mock_jinja = mocker.MagicMock()
    mock_jinja.loader.get_source.return_value = '{% block drop %}drop table {{ tmp }}; {% endblock %}'
    mock_jinja.parse = jinja2.Environment().parse
    variables = MartOperator.get_template_variables(mock_jinja, 'test')

    assert variables == {'tmp'}


def test_get_template_variables__multiple_tmp_tables__correct_list_returned(mocker: MockerFixture):
    mock_jinja = mocker.MagicMock()
    mock_jinja.loader.get_source.return_value = (
        '{% block drop__1 %}drop table {{ tmp1 }}; {% endblock %}'
        '{% block drop__2 %}drop table {{ tmp2 }}; {% endblock %}'
        '{% block drop__final %}drop table {{ tmp_final }}; {% endblock %}'
    )
    mock_jinja.parse = jinja2.Environment().parse
    variables = MartOperator.get_template_variables(mock_jinja, 'test')

    assert variables == {'tmp1', 'tmp2', 'tmp_final'}


def test_get_template_variables__no_variables__correct_list_returned(mocker: MockerFixture):
    mock_jinja = mocker.MagicMock()
    mock_jinja.loader.get_source.return_value = ''
    mock_jinja.parse = jinja2.Environment().parse
    variables = MartOperator.get_template_variables(mock_jinja, 'test')

    assert variables == set()


def test_get_template_variables__single_tmp_table__correct_tmp_tables_built(mocker: MockerFixture):
    template = (
        '{% block create_temp %} hello from {{ table }} {% endblock %}'
        '{% block drop_temp %} drop table {{ tmp }}; {% endblock %}'
    )
    operator = operator_factory(mocker, MartMethod.REPLACE, template)

    assert operator.tmp_tables == ['tmp']


def test_get_template_variables__multiple_tmp_tables__correct_tmp_tables_built(mocker: MockerFixture):
    template = (
        '{% block create_temp %} create table {{ schema }}.{{ table }}; {% endblock %}'
        '{% block drop_temp__1 %}drop table {{ tmp1 }}; {% endblock %}'
        '{% block drop_temp__2 %}drop table {{ tmp2 }}; {% endblock %}'
        '{% block drop_temp__final %}drop table {{ tmp_final }}; {% endblock %}'
    )
    operator = operator_factory(mocker, MartMethod.REPLACE, template)

    assert len(operator.tmp_tables) == 3
    assert set(operator.tmp_tables) == {'tmp1', 'tmp_final', 'tmp2'}


def test_get_template_variables__multiple_tmp_tables_without_final__exception_raised(mocker: MockerFixture):
    template = (
        '{% block create_temp %} create table {{ schema }}.{{ table }}; {% endblock %}'
        '{% block drop_temp__1 %}drop table {{ tmp1 }}; {% endblock %}'
        '{% block drop_temp__2 %}drop table {{ tmp2 }}; {% endblock %}'
        '{% block drop_temp__final %}drop table {{ tmp3 }}; {% endblock %}'
    )
    with pytest.raises(DWHError):
        operator_factory(mocker, MartMethod.REPLACE, template)


def test_get_template_variables__no_tmp_tables__correct_tmp_tables_built(mocker: MockerFixture):
    template = '{% block create_temp %} create table {{ schema }}.{{ table }}; {% endblock %}'
    operator = operator_factory(mocker, MartMethod.REPLACE, template)

    assert operator.tmp_tables == []

@pytest.mark.parametrize(
    'stage_name, expected_stage, expected_substage_name',
    [
        ('create_temp', Stage.CREATE_TEMP, None),
        ('INSERT_TEMP_TO_TARGET', Stage.INSERT_TEMP_TO_TARGET, None),
        ('drop_temp__tEST_name', Stage.DROP_TEMP, 'test_name'),
    ],
)
def test_template_block_to_substage__valid_block__built_correctly(
    mocker: MockerFixture, stage_name: str, expected_stage: Stage, expected_substage_name: str
):
    operator = operator_factory(mocker, MartMethod.REPLACE)
    template = jinja2.Template(f'{{%- block {stage_name} -%}} hello {{%- endblock -%}}')

    stage, substage_name, substage_callable = operator.template_block_to_substage(
        'test_template', stage_name, template.blocks[stage_name]
    )
    assert stage == expected_stage
    assert substage_name == expected_substage_name
    assert hasattr(substage_callable, '__call__')


def test_template_block_to_substage__invalid_method_stage__exception_raised(mocker: MockerFixture):
    operator = operator_factory(mocker, MartMethod.REPLACE)
    stage_name = 'merge_temp_to_target'
    template = jinja2.Template(f'{{%- block {stage_name} -%}} hello {{%- endblock -%}}')

    with pytest.raises(DWHError):
        operator.template_block_to_substage('test_template', stage_name, template.blocks[stage_name])


def test_template_block_to_substage__invalid_stage__exception_raised(mocker: MockerFixture):
    operator = operator_factory(mocker, MartMethod.REPLACE)
    stage_name = 'well_im_definitely_not_stage_name'
    template = jinja2.Template(f'{{%- block {stage_name} -%}} hello {{%- endblock -%}}')

    with pytest.raises(DWHError):
        operator.template_block_to_substage('test_template', stage_name, template.blocks[stage_name])


def test_prepare_stages__all_stages_filled__stages_prepared(mocker: MockerFixture):
    template = '''
        {%- block create_temp -%} hello from create_temp {%- endblock -%}
        {%- block truncate_target -%} hello from truncate_target {%- endblock -%}
        {%- block insert_temp_to_target -%} hello from insert_temp_to_target {%- endblock -%}
        {%- block drop_temp -%} hello from drop_temp {%- endblock -%}
    '''
    mock_block_factory = mocker.patch('airflow_extensions.operators.mart_operator.MartOperator.block_runner_factory')
    operator = operator_factory(mocker, MartMethod.REPLACE, template=template)

    stages = [stage for stage, _ in operator.stages]
    assert stages == MartOperator.method_stages[MartMethod.REPLACE]
    assert mock_block_factory.call_count == 4

    built_block_runners = [args[1] for args, kwargs in mock_block_factory.call_args_list]
    assert built_block_runners == ['create_temp', 'truncate_target', 'insert_temp_to_target', 'drop_temp']


def test_prepare_stages__stage_with_default_missing__default_used(mocker: MockerFixture):
    template = '''
        {%- block create_temp -%} hello from create_temp {%- endblock -%}
        {%- block merge_temp_to_target -%} hello from merge_temp_to_target {%- endblock -%}
    '''
    mock_block_factory = mocker.patch('airflow_extensions.operators.mart_operator.MartOperator.block_runner_factory')
    operator = operator_factory(mocker, MartMethod.MERGE, template=template)
    prepared_stages = operator.stages

    stages = [stage for stage, _ in prepared_stages]
    assert stages == MartOperator.method_stages[MartMethod.MERGE]
    assert mock_block_factory.call_count == 2

    drop_temp_callables, *_ = [stg_cb for stage, stg_cb in prepared_stages if stage == Stage.DROP_TEMP]
    assert len(drop_temp_callables) == 1
    _, drop_temp_callable = drop_temp_callables[0]
    assert drop_temp_callable is MartOperator.default_stages[Stage.DROP_TEMP]

    built_block_runners = [args[1] for args, kwargs in mock_block_factory.call_args_list]
    assert built_block_runners == ['create_temp', 'merge_temp_to_target']


def test_prepare_stages__stage_with_multiple_substages__all_stages_present(mocker: MockerFixture):
    template = '''
        {%- block create_temp__one -%} hello from create_temp__one {%- endblock -%}
        {%- block create_temp__two -%} hello from create_temp__two {%- endblock -%}
        {%- block insert_temp_to_target -%} hello from insert_temp_to_target {%- endblock -%}
    '''
    operator = operator_factory(mocker, MartMethod.INCREMENT, template=template, report_dt_column='test')

    flat_stages = [(stage, substage_name) for stage, substages in operator.stages for substage_name, _ in substages]
    assert flat_stages == [
        (Stage.CREATE_TEMP, 'one'),
        (Stage.CREATE_TEMP, 'two'),
        (Stage.DELETE_INCREMENT_TARGET, None),
        (Stage.INSERT_TEMP_TO_TARGET, None),
        (Stage.DROP_TEMP, None),
    ]


def test_prepare_stages__required_stage_missing__exception_raised(mocker: MockerFixture):
    template = '''
        {%- block delete_increment_target -%} hello from delete_increment_target {%- endblock -%}
        {%- block insert_temp_to_target -%} hello from insert_temp_to_target {%- endblock -%}
    '''
    with pytest.raises(DWHError):
        operator_factory(mocker, MartMethod.INCREMENT, report_dt_column='test', template=template)


@pytest.mark.parametrize(
    'column_max, expected_result',
    [
        (pendulum.datetime(2020, 4, 20).to_datetime_string(), pendulum.date(2020, 4, 20)),
        (pendulum.date(2020, 4, 21).to_date_string(), pendulum.date(2020, 4, 21)),
        (None, None),
    ],
)
def test_get_max_dt_column_date__datetime_returned__date_returned(mocker: MockerFixture, column_max, expected_result):
    operator = operator_factory(mocker, MartMethod.INCREMENT, report_dt_column='test')
    mocker.patch(
        'airflow_extensions.operators.mart_operator.exasol.get_column_max',
        return_value=column_max,
    )
    max_dt_column_date = operator.get_max_dt_column_date(mocker.MagicMock)

    assert max_dt_column_date == expected_result


def test_get_max_dt_column_date__no_report_dt_column__none_returned(mocker: MockerFixture):
    operator = operator_factory(mocker, MartMethod.MERGE)
    max_dt_column_date = operator.get_max_dt_column_date(mocker.MagicMock)

    assert max_dt_column_date is None


def test_get_date_bounds__empty_table__lower_bound_is_min_date(mocker: MockerFixture):
    data_interval_start = pendulum.DateTime(2020, 4, 20)
    min_date = pendulum.DateTime(2020, 1, 1)
    operator = operator_factory(mocker, MartMethod.INCREMENT, report_dt_column='test', min_date=min_date)
    lower_bound, upper_bound = operator.get_date_bounds(data_interval_start, 60)

    assert lower_bound == data_interval_start
    assert upper_bound == pendulum.DateTime(2020, 4, 20, 1)

    lower_bound, upper_bound = operator.get_date_bounds(data_interval_start, 1440)

    assert lower_bound == data_interval_start
    assert upper_bound == pendulum.DateTime(2020, 4, 21)

def test_get_date_bounds__no_dt_column__lower_bound_is_execution_date(mocker: MockerFixture):
    data_interval_start = pendulum.DateTime(2020, 4, 20)
    operator = operator_factory(mocker, MartMethod.REPLACE)

    lower_bound, upper_bound = operator.get_date_bounds(data_interval_start, 60)

    assert lower_bound == data_interval_start
    assert upper_bound == pendulum.DateTime(2020, 4, 20, 1)

    lower_bound, upper_bound = operator.get_date_bounds(data_interval_start, 1440)

    assert lower_bound == data_interval_start
    assert upper_bound == pendulum.DateTime(2020, 4, 21)

@pytest.mark.parametrize(
    'lag, max_dt_column_date, expected_lower_bound',
    [
        (timedelta(days=5), pendulum.date(2020, 4, 14), pendulum.DateTime(2020, 4, 15)),
        (timedelta(days=5), pendulum.date(2020, 4, 17), pendulum.DateTime(2020, 4, 15)),
    ],
)
def test_get_date_bounds__has_dt_column_and_lag__lower_bound_is_min_of_execution_date_and_max_business_date(
    mocker: MockerFixture, lag: timedelta, max_dt_column_date: pendulum.Date, expected_lower_bound: pendulum.Date
):
    data_interval_start = pendulum.DateTime(2020, 4, 20)
    operator = operator_factory(mocker, MartMethod.REPLACE, report_dt_column='some_column', lag=lag)
    mocker.patch.object(operator, 'get_max_dt_column_date', return_value=max_dt_column_date)

    lower_bound, upper_bound = operator.get_date_bounds(data_interval_start, 60)

    assert lower_bound == expected_lower_bound
    assert upper_bound == pendulum.DateTime(2020, 4, 20, 1)

    lower_bound, upper_bound = operator.get_date_bounds(data_interval_start, 1440)

    assert lower_bound == expected_lower_bound
    assert upper_bound == pendulum.DateTime(2020, 4, 21)

def test_execute__mock_stage_callables__stages_called_with_correct_context_and_order(mocker: MockerFixture):
    template = '''
        {%- block create_temp__one -%} hello from create_temp__one {%- endblock -%}
        {%- block create_temp__two -%} hello from create_temp__two {%- endblock -%}
        {%- block merge_temp_to_target -%} hello from merge_temp_to_target {%- endblock -%}
    '''
    min_date = pendulum.DateTime(2020, 1, 1)
    expected_context = StageContext(
        jinja2.Template(template),
        schema='test_schema',
        table='test_table',
        lower_bound=min_date,
        upper_bound=pendulum.DateTime(2020, 1, 1, 1),
        report_dt_column='test',
        debug=False,
    )
    operator = operator_factory(
        mocker,
        MartMethod.MERGE,
        template=template,
        min_date=min_date,
        report_dt_column='test',
        params={'discreteness': 60},
    )
    mocker.patch('airflow_extensions.operators.mart_operator.exasol.is_table_empty', return_value=True)
    mock_hook = mocker.patch('airflow_extensions.operators.mart_operator.ExasolHook')
    mock_conn = mock_hook().get_conn().__enter__()
    substages_spy = mocker.MagicMock()
    # Подменяем stage_callable на поле substages_spy (MagicMock) чтобы можно было отследить порядок вызова и аргументы
    operator.stages = [
        (
            stage,
            [
                (
                    substage_name,
                    substages_spy.__getattr__(f'{stage}{"__" + substage_name if substage_name is not None else ""}'),
                )
                for substage_name, substage_callable in substages
            ],
        )
        for stage, substages in operator.stages
    ]
    operator.execute(
        {'data_interval_start': pendulum.DateTime(2020, 1, 1), 'debug': False, 'params': {'discreteness': 60}}
    )

    substages_spy.assert_has_calls(
        [
            mocker.call.create_temp__one(mock_conn, expected_context),
            mocker.call.create_temp__two(mock_conn, expected_context),
            mocker.call.merge_temp_to_target(mock_conn, expected_context),
            mocker.call.drop_temp(mock_conn, expected_context),
        ]
    )


def test_execute__mock_connection__correct_sql_executed_in_order(mocker: MockerFixture):
    template = '''
        {%- block create_temp__one -%} hello from create_temp__one {{ lower_bound }} {%- endblock -%}
        {%- block create_temp__two -%} hello from create_temp__two {{ upper_bound }} {%- endblock -%}
        {%- block merge_temp_to_target -%} hello from merge_temp_to_target {{ report_dt_column }} {%- endblock -%}
        {%- block drop_temp -%} hello from drop_temp {{ schema }}.{{ table }} {%- endblock -%}
    '''
    min_date = pendulum.DateTime(2020, 1, 1)
    operator = operator_factory(
        mocker,
        MartMethod.MERGE,
        template=template,
        min_date=min_date,
        report_dt_column='test',
        params={'discreteness': 60},
    )
    mocker.patch('airflow_extensions.operators.mart_operator.exasol.is_table_empty', return_value=True)
    mock_hook = mocker.patch('airflow_extensions.operators.mart_operator.ExasolHook')

    operator.execute(
        {'data_interval_start': pendulum.DateTime(2020, 1, 1), 'debug': False, 'params': {'discreteness': 60}}
    )

    mock_conn = mock_hook().get_conn().__enter__()
    mock_conn.assert_has_calls(
        [
            mocker.call.commit(),
            mocker.call.execute('hello from create_temp__one 2020-01-01 00:00:00'),
            mocker.call.execute().close(),
            mocker.call.commit(),
            mocker.call.execute('hello from create_temp__two 2020-01-01 01:00:00'),
            mocker.call.execute().close(),
            mocker.call.commit(),
            mocker.call.execute('hello from merge_temp_to_target test'),
            mocker.call.execute().close(),
            mocker.call.execute('hello from drop_temp test_schema.test_table'),
            mocker.call.execute().close(),
            mocker.call.commit(),
        ]
    )
