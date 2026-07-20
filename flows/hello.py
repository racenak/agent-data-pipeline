from prefect import flow, task

@task
def hello():
    print("Hello")

@flow
def hello_flow():
    hello()

if __name__ == "__main__":
    hello_flow()
