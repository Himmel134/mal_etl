from prefect import flow
from prefect_github import GitHubRepository  # ini yang benar
from prefect.client.schemas.schedules import CronSchedule

if __name__ == "__main__":
    github_repo = GitHubRepository.load("github-storage")  # pastikan block ini ada di Prefect UI
    flow.from_source(
        source=github_repo,
        entrypoint="mal_etl.py:mal_etl_mainflow",  # sesuaikan nama file dan flow-nya
    ).deploy(
        name="mal-etl-deployment",
        schedule=CronSchedule(cron="0 9 * * *", timezone="Asia/Jakarta"),
        work_pool_name="etl-workers",
        tags=["mal", "etl"]
    )