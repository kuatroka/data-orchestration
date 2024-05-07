import time
import asyncio

# traditional function
# def my_function():
#     print("Start of the function")
#     time.sleep(1)
#     print("End of the function")

# # coroutine
# async def my_coroutine():
#     print("Start of the coroutine")
#     await asyncio.sleep(1)
#     print("End of the coroutine")

async def coroutine_1():
    print(f"\ncoroutine_1 Started at: {time.ctime()}\n")
    print("Start of coroutine_1")
    await asyncio.sleep(5)
    print("End of coroutine_1")
    print(f"\ncoroutine_1 Finished at: {time.ctime()}\n")

async def coroutine_2():
    print(f"\ncoroutine_2 Started at: {time.ctime()}\n")
    print("Start of coroutine_2")
    await asyncio.sleep(9)
    print("End of coroutine_2")
    print(f"\ncoroutine_2 Finished at: {time.ctime()}\n")

# call coroutines with asyncio.gather
async def all_coroutines():
    coroutines = [coroutine_1(), coroutine_2()]
    await asyncio.gather(*coroutines)




if __name__ == "__main__":

    # my_function()  # call function
    # asyncio.run(my_coroutine())  # call coroutine
    asyncio.run(all_coroutines())