import httpx
from prefect import flow, task, get_run_logger
from prefect_dask.task_runners import DaskTaskRunner

@task
def retreive_from_api(
    base_url: str ,
    path: str,
    secure: bool
):
    logger = get_run_logger()
    if secure:
        url = f'{base_url}{path}'
    else:
        url = f'http://{base_url}{path}'
    
    response = httpx.get(url)
    response.raise_for_status()
    inventory_stats = response.json()
    logger.info(inventory_stats)

@task
def retreive_from_api_emoji(
    base_url_emoji: str ,
    path_emoji: str,
):
    logger = get_run_logger()
    url = f'{base_url_emoji}/{path_emoji}'    
    response = httpx.get(url)
    response.raise_for_status()
    emoji_stats = response.json()
    logger.info(emoji_stats)

@task
def retreive_from_api_emoji2(
    base_url_emoji: str ,
    path_emoji: str,
):
    logger = get_run_logger()
    url = f'{base_url_emoji}/{path_emoji}'    
    response = httpx.get(url)
    response.raise_for_status()
    emoji_stats = response.json()
    logger.info(emoji_stats)

@flow(task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": 3}))
def collect_petstore_inventory(
    base_url: str = 'https://petstore.swagger.io',
    path: str = '/v2/store/inventory',
    secure: bool = True,
    base_url_emoji: str = 'https://emojihub.yurace.pro',
    path_emoji: str = 'api/random'
):
    inventory_stats = retreive_from_api.submit(
        base_url=base_url,
        path=path,
        secure=secure
    )

    emoji_stats = retreive_from_api_emoji.submit(
        base_url_emoji=base_url_emoji,
        path_emoji=path_emoji        
    )
    

    emoji_stats2 = retreive_from_api_emoji2(
        base_url_emoji=base_url_emoji,
        path_emoji=path_emoji        
    )










def main():
    collect_petstore_inventory.serve('petstore-collection-deployment')

if __name__ == "__main__":
    main()