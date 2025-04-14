from prefect.filesystems import GitHubRepository
from prefect.client.schemas.schedules import CronSchedule
from prefect import flow

# Load block GitHubRepository kamu
github_repo = GitHubRepository.load("github-storage")  # ganti kalau nama block-nya berbeda

# Deploy flow dari GitHub dengan jadwal
flow.from_source(
    source=github_repo,
    entrypoint="mal_etl.py:mal_etl_mainflow",  # pastikan nama file dan nama flow sesuai
).deploy(
    name="mal-etl-deployment",  # nama deployment
    schedule=CronSchedule(cron="0 9 * * *", timezone="Asia/Jakarta"),  # tiap hari jam 9 pagi WIB
    work_pool_name="etl-workers",  # ganti sesuai nama work pool kamu di Prefect UI
    tags=["mal", "etl"]  # opsional
)