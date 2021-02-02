from apscheduler.schedulers.background import BackgroundScheduler
from covid_IB import get_csv
import logging
from git import Repo, GitCommandError

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
PATH_OF_GIT_REPO = r'.git'
COMMIT_MESSAGE = 'data auto updated'


def push_data():
    if get_csv("data/", "download/"):
        logging.info("Data updated")
        git_push("data/")
        git_push("download/")
        git_push("arcgis_cvs/")
        return True


def git_push(data_folder="data/"):
    try:
        logging.info(f"Pushing {data_folder}")
        repo = Repo(PATH_OF_GIT_REPO)
        repo.git.add(data_folder)
        repo.index.commit(COMMIT_MESSAGE)
        origin = repo.remote(name='origin')
        origin.push()
    except GitCommandError as e:
        logging.info(f'Some error occured while pushing the code: {e}')


def main():
    scheduler = BackgroundScheduler()
    scheduler.add_job(push_data, "interval", hours=1)
    scheduler.start()
    logging.info("Started")


if __name__ == "__main__":
    main()
    # push_data()
    # git_push("data/")
    # git_push("download/")
    # git_push("arcgis_cvs/")
