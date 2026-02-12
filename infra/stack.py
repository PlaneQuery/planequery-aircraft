import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_ecr_assets,
    aws_iam as iam,
    aws_logs as logs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct
from pathlib import Path


class AdsbProcessingStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # --- S3 bucket for intermediate and final results ---
        bucket = s3.Bucket(
            self, "ResultsBucket",
            bucket_name="planequery-aircraft-dev",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    prefix="intermediate/",
                    expiration=Duration.days(7),
                )
            ],
        )

        # --- Use default VPC (no additional cost) ---
        vpc = ec2.Vpc.from_lookup(
            self, "Vpc",
            is_default=True,
        )

        # --- ECS Cluster ---
        cluster = ecs.Cluster(
            self, "Cluster",
            vpc=vpc,
            container_insights=True,
        )

        # --- Log group ---
        log_group = logs.LogGroup(
            self, "LogGroup",
            log_group_name="/adsb-processing",
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.TWO_WEEKS,
        )

        # --- Docker images (built from local Dockerfiles) ---
        adsb_dir = str(Path(__file__).parent.parent / "src" / "adsb")

        worker_image = ecs.ContainerImage.from_asset(
            adsb_dir,
            file="Dockerfile.worker",
            platform=cdk.aws_ecr_assets.Platform.LINUX_ARM64,
        )
        reducer_image = ecs.ContainerImage.from_asset(
            adsb_dir,
            file="Dockerfile.reducer",
            platform=cdk.aws_ecr_assets.Platform.LINUX_ARM64,
        )

        # --- Task role (shared) ---
        task_role = iam.Role(
            self, "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )
        bucket.grant_read_write(task_role)

        # --- MAP: worker task definition ---
        map_task_def = ecs.FargateTaskDefinition(
            self, "MapTaskDef",
            cpu=4096,           # 4 vCPU
            memory_limit_mib=30720,  # 30 GB
            task_role=task_role,
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.ARM64,
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
        )
        map_container = map_task_def.add_container(
            "worker",
            image=worker_image,
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="map",
                log_group=log_group,
            ),
            environment={
                "S3_BUCKET": bucket.bucket_name,
            },
        )

        # --- REDUCE: reducer task definition ---
        reduce_task_def = ecs.FargateTaskDefinition(
            self, "ReduceTaskDef",
            cpu=4096,            # 4 vCPU
            memory_limit_mib=30720,  # 30 GB — must hold full year in memory
            task_role=task_role,
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.ARM64,
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
        )
        reduce_container = reduce_task_def.add_container(
            "reducer",
            image=reducer_image,
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="reduce",
                log_group=log_group,
            ),
            environment={
                "S3_BUCKET": bucket.bucket_name,
            },
        )

        # --- Step Functions ---

        # Map task: run ECS Fargate for each date chunk
        map_ecs_task = sfn_tasks.EcsRunTask(
            self, "ProcessChunk",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=cluster,
            task_definition=map_task_def,
            launch_target=sfn_tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST,
            ),
            container_overrides=[
                sfn_tasks.ContainerOverride(
                    container_definition=map_container,
                    environment=[
                        sfn_tasks.TaskEnvironmentVariable(
                            name="START_DATE",
                            value=sfn.JsonPath.string_at("$.start_date"),
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="END_DATE",
                            value=sfn.JsonPath.string_at("$.end_date"),
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="RUN_ID",
                            value=sfn.JsonPath.string_at("$.run_id"),
                        ),
                    ],
                )
            ],
            assign_public_ip=True,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            result_path="$.task_result",
        )

        # Map state — max 3 concurrent workers
        map_state = sfn.Map(
            self, "FanOutChunks",
            items_path="$.chunks",
            max_concurrency=3,
            result_path="$.map_results",
        )
        map_state.item_processor(map_ecs_task)

        # Reduce task: combine all chunk CSVs
        reduce_ecs_task = sfn_tasks.EcsRunTask(
            self, "ReduceResults",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=cluster,
            task_definition=reduce_task_def,
            launch_target=sfn_tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST,
            ),
            container_overrides=[
                sfn_tasks.ContainerOverride(
                    container_definition=reduce_container,
                    environment=[
                        sfn_tasks.TaskEnvironmentVariable(
                            name="RUN_ID",
                            value=sfn.JsonPath.string_at("$.run_id"),
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="GLOBAL_START_DATE",
                            value=sfn.JsonPath.string_at("$.global_start_date"),
                        ),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="GLOBAL_END_DATE",
                            value=sfn.JsonPath.string_at("$.global_end_date"),
                        ),
                    ],
                )
            ],
            assign_public_ip=True,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
        )

        # Chain: fan-out map → reduce
        definition = map_state.next(reduce_ecs_task)

        sfn.StateMachine(
            self, "Pipeline",
            state_machine_name="adsb-map-reduce",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(48),
        )

        # --- Outputs ---
        cdk.CfnOutput(self, "BucketName", value=bucket.bucket_name)
        cdk.CfnOutput(self, "StateMachineName", value="adsb-map-reduce")
