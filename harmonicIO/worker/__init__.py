import importlib

falcon_spec = importlib.util.find_spec("falcon")
if falcon_spec is None:
    raise Exception("Falcon module has not been installed.")

docker_spec = importlib.util.find_spec("docker")
if docker_spec is None:
    raise Exception("Docker module has not been installed.")
