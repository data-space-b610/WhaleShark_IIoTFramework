# KSB Dockerize

`ksblib.dockerize` creates a docker image wrapping the user-provided functions in Python and then attaches REST API for input and/or output data exchanges.

## Dependency

- Docker 18+
- Python 3.5+

## How to User in Python


### 1. Generate base.py and main_func

Let's start with the assumption that we have custom Python scripts under the "modules" folder structured as follows:

```sh
# Example of user-provided folder structure. 
-+ modules
    - __init__.py
    - base.py
    - requirements.txt
    - user_python_file_01.py
    - datafile01.pkl
    - ...
    -+ libraries
        - library01.py
        - ...
```

In order to dockerize the above folder with attached REST API, `__init__.py` and `base.py` is mandatory. `__init.py__` can be an empty Python file but must exist. `base.py` must contain a main function named `main_func`. For example,

```python
def main_func(x):
    # x is in the type of Python dictionary.
    input = x['input']

    # Do some work with the input data.
    squared_input = input * input

    return {'output': squared_input, 'message': 'test'}
```

`ksb.dockerize` will execute this `main_func`. `x` is an input data in Python dictionary format. For instance `x` could be:

```python
{'input': 4,  
 'flag': '0100',
 'hdfs': {
          'url': 'http://localhost:9090',
          'user': 'ksb'
         }
}
``` 

It is up to you how to design `x` and use it in the `base.py`. `main_func` must return outputs in Python dictionary format as well. `ksblib.dockerize` will return the output in string format.


### 2. Test `main_func`

Please note that `main_func` must be tested by on your own before using `ksblib.dockerize` library. To do that, open a python console at the parent folder of the "modules" folder. Then type as follows:

```python
from modules.base import main_func

x = {'input': 4}
main_func(x)
```

The above code should return the defined output without raising any errors. 


### 3. Dockerize the Custom Python Scripts

You can then use `ksblib.dockerize` as follows:

```python
from ksblib.dockerize import Dockerize

drize = Dockerize('docker_image_name', '/path/to/modules', 
                  requirements=['numpy:scipy>=1.1.0'],  
                  user_args='any_string_values')
drize.build()
drize.run()
```

This will build a custom docker image containing the "modules" folder and run the image. It also attaches REST API in order to get input data in the json format.  By default, `ksblib.dockerize` maps the REST API to 8080 port. Other port also can be used, such as `drize.run(port=9090)`. Note that `/path/to/modules` can be either local path or HDFS path (i.e. starting with `hdfs://`). If it is a local path, it must be an absolute path (i.e. starting with `/`). In the case of HDFS path, HDFS environment must be installed beforehand (i.e. `hdfs` command). 

For installation of python dependencies, if exists, a list of needed python libraries separated by ":" can be provided (e.g. `requirements=['numpy:scipy>=1.1.0']`). This parameter is optional. You can also create "requirements.txt" in the "modules" folder. You can list needed python libraries in the file as follows:

```sh
numpy
scipy>=1.1.0
``` 

`ksb.dockerize` install libraries in the "requirements.txt" if exist, and also install libraries from the argument: `requirements` if provided.

If you need to use pre-defined parameters that you do not want to ship with the input data `x`, you can use an optional `user_args` parameter. The string value of `user_args` is saved to `ksb_user_args.pkl` when building the docker image and can be accessed as:

```python
# Any python script in the "modules" folder.
def read_user_args():
    import os
    import pickle
    
    # Read the pickle file.
    dir_name = os.path.dirname(__file__)
    user_args = pickle.load(open(os.path.join(dir_name, 
                            'ksb_user_args.pkl'), 'rb'))
    
    return user_args
``` 

Note that the file `ksb_user_args.pkl` is saved at the root folder (i.e. "modules" in this example).

If you already have built a docker image using `ksb.dockerize` and do not want to rebuilt it, comment the line `drize.build()`. It will then run the exist docker image with the same name (i.e. `docker_image_name` in this example).


### 4. Query the Built Docker Image 

To query the built docker image, do as follows:

```
$> curl -d '{"input": 4}' http://localhost:8080
```

After commit of the above command, the following tasks are sequentially executed.

1. `curl` command sends `{"input": 4}` to the built Docker image through REST API.
2. `ksb.dockerize` converts the received input value to Python dictionary.
3. `ksb.dockerize` transmits the converted dictionary to `main_func`.
4. `main_func` returns `{'output': 16, 'message': 'test'}`. 


<!---
### Use `ksblib.dockerize` in Command Line

This library can be used from the command line as follows:
```bash
python ksblib/dockerize/call_dockerize.py docker_image_name /path/to/modules --python_libraries nltk:scipy==1.0 --port 8080 --user_args http://localhost:9090/predict
```

Or If you have files in HDFS,

```bash
python ksblib/dockerize/call_dockerize.py docker_image_name hdfs://localhost:9000/path/to/modules --python_libraries nltk:scipy==1.0 --port 8080 --user_args=http://localhost:9090/predict
```

Note that the parameters starting with double-dash (i.e. `--`) are optional. Also, be careful with using `--python_libraries`. You should not user `>` or `<` for library versions since such special characters cannot be ingested from the command line to the python script. 
-->


## With KSB Framework

To use `ksblib.dockerize` in the KSB Framework, returned dictionary type must not be nested. For instance, the following code is OK.

```python
# This is OK.
return {'output': 'Hi', 'message': 'test'}
```

But this is not.

```python
# This is NOT OK.
return {'output': {'name': 'Kim', 'sex': 'male'}}
```


