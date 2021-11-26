sgepy
=====

Simple package for simple SGE job submission & monitoring

# Examples

Using a simple lambda function

```
func = lambda x: [x**2 for x in range(5)]
w = SGE.Workerverbose=True)
w(func, 2)
```

Test with keyword arguments and package dependencies

```
# simple function
def func1(x, y=1, z=2):
    time.sleep(x)
    return x * y * z

# cluster worker 
kwargs = {'y' : 2, 'z' : 3}
pkgs = ['time']
w = SGE.Worker(tmp_dir=args.tmp_dir, kwargs=kwargs, pkgs=pkgs, verbose=True)
w(func1, 1)
```

Using the `multiprocessing.Pool()` functionality

```
# simple function (requires import of a package)
def func1(x, y=1, z=2):
    time.sleep(x)
    return x * y * z
    
# map call
kwargs = {'y' : 2, 'z' : 2}
pkgs = ['time']
p = SGE.Pool(tmp_dir=args.tmp_dir, kwargs=kwargs, pkgs=pkgs, n_jobs=2, verbose=True)
p.map(func1, [1,5])
```

