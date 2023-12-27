# Python Taurus Queue Publisher

[![Latest Version](https://img.shields.io/github/v/release/not-empty/taurus-publisher-python.svg?style=flat-square)](https://github.com/not-empty/taurus-publisher-python/releases)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)

Python library using LUA script to send for redis a job for Taurus queue

### Installation

[Release 1.0.2](https://github.com/not-empty/taurus-publisher-python/releases/tag/1.0.0) Requires [Python](https://www.python.org/) 3.6.9

The recommended way to install is through [Pip](https://pypi.org/project/pip/).

```sh
pip3 install taurus-publisher
```

### Usage

Publishing

```python
from taurus_publisher.publisher import Publisher
import asyncio

async def test_publish():
    publisher = Publisher()

    data = '{"python":1}'

    job_id = await publisher.add_job(
        queue="python",
        data=data,
        opts={},
        name="process"
    )

    print(f"Job added with ID: {job_id}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_publish())
    loop.close()
```

### Development

Want to contribute? Great!

The project using a simple code.
Make a change in your file and be careful with your updates!
**Any new code will only be accepted with all validations.**

**Not Empty Foundation - Free codes, full minds**