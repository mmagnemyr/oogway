from logging import Logger
import string
from emworks_templates import stringtemplate


class SQL():
    target_bucket=""
    target_table_name=""
    drop_table=""
    custom_source_view=""
    source_view_name=""
    stage_view=""
    create_target_table=""
    insert_missing_member=""
    populate_data=""
    populate_data_incemental=""
    update_iscurrent_0=""
    update_iscurrent_1=""

    database_name=""
    table_name=""
    
    drop_stage_view2="DROP"
    
    drop_source_view:string="DROP"
    optimize_table=""
    
    verify_wk_unique=""
    verify_bk_unique=""
    verify_wk_not_null=""
    verify_partition_row_count=""


    unload_table=""
    unload_table_steps_name=""
    add_partition=""

    full_path=""
    bucket=""
    prefix=""
    athena_s3_location=""

class AWSStepsBuilder:
    aws_region="eu-west-1"
    aws_account=""

  