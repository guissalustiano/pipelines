# -*- coding: utf-8 -*-
"""
Flows for br_anp_precos_combustiveis
"""

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_anp_precos_combustiveis.schedules import (
    every_week_anp_microdados,
)
from pipelines.datasets.br_anp_precos_combustiveis.tasks import (
    check_for_updates,
    data_max_bd_pro,
    download_and_transform,
    make_partitions,
)
from pipelines.datasets.br_anp_precos_combustiveis.utils import download_files
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="br_anp_precos_combustiveis.microdados", code_owners=["trick"]
) as anp_microdados:
    dataset_id = Parameter(
        "dataset_id", default="br_anp_precos_combustiveis", required=True
    )
    table_id = Parameter("table_id", default="microdados", required=True)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    update_metadata = Parameter("update_metadata", default=True, required=False)

    dados_desatualizados = check_for_updates(dataset_id=dataset_id, table_id=table_id)
    log_task(f"Checando se os dados estão desatualizados: {dados_desatualizados}")
    with case(dados_desatualizados, False):
        log_task(
            "Dados atualizados, não é necessário fazer o download",
            upstream_tasks=[dados_desatualizados],
        )

    with case(dados_desatualizados, True):
        df = download_and_transform(upstream_tasks=[rename_flow_run])
        output_path = make_partitions(df=df, upstream_tasks=[df])

        # pylint: disable=C0103
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_path,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
                upstream_tasks=[wait_upload_table],
            )

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )
            get_date_max_pro = data_max_bd_pro(
                df=df, upstream_tasks=[wait_upload_table]
            )
            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id,
                    table_id,
                    metadata_type="DateTimeRange",
                    bq_last_update=False,
                    bq_table_last_year_month=False,
                    api_mode="prod",
                    date_format="yy-mm-dd",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="weeks",
                    _last_date=get_date_max_pro,
                    upstream_tasks=[get_date_max_pro],
                )
anp_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
anp_microdados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
anp_microdados.schedule = every_week_anp_microdados
