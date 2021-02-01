import git
import os
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)


def repository_last_changes(base_directory="../download/covid19_IB/", watch_file="covid19_IB.csv"):
    file = f"{base_directory}{watch_file}"
    mtime = os.path.getmtime(file)
    return mtime


def hospital_has_changes(base_directory="../download/covid19_IB/",):
    # check if repository exists
    if not os.path.exists(base_directory):
        os.makedirs(base_directory)
        git.Repo.clone_from("https://github.com/druizaguilera/covid19_IB", base_directory)
        logging.info("Git: New data found")
        return True
    original_mtime = repository_last_changes(base_directory, "covid19_IB.csv")

    # update repository
    g = git.cmd.Git(base_directory)
    try:
        g.pull()
    except git.GitCommandError as e:
        logging.info("Error pulling data from covid19_IB: " + e.stderr)
        try:
            g.reset('--hard', 'origin/master')
            g.pull()
            logging.info("Error solved with reset --hard")
        except git.GitCommandError as e:
            raise e
    # check if files changed
    new_mtime = repository_last_changes(base_directory, "covid19_IB.csv")
    if original_mtime != new_mtime:
        logging.info("Git: Data updated")
        return True

    logging.info("Git: No New data")
    return False


if __name__ == "__main__":
    hospital_has_changes("../download/covid19_IB/")
