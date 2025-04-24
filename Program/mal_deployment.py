from prefect import flow
from prefect_github import GitHubRepository
from prefect.client.schemas.schedules import CronSchedule

if __name__ == "__main__":
    github_repo = GitHubRepository.load("github-storage")
    flow.from_source(
        source=github_repo,
        entrypoint="Program/mal_etl.py:mal_etl_mainflow",
    ).deploy(
        name="mal-etl-deployment",
        schedule=CronSchedule(cron="0 9 * * *", timezone="Asia/Jakarta"),
        work_pool_name="etl-workers",
        tags=["mal", "etl"]
    )