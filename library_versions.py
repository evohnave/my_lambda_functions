import pkg_resources
import numpy as np     # Must be in a layer
import scipy           # Must be in a layer

def lambda_handler(event, context):
    for dist in list(pkg_resources.working_set):
        print(dist)
    print(f"numpy version is {np.__version__}")
    print(f"scipy version is {scipy.__version__}")

  
