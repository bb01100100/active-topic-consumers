import yaml
import re
import os


def load_config(file):
    """Loads a yaml file replacing any ${VAR} values with var values
    from the environment.
    """

    path_matcher = re.compile(r'.*\$\{([^}^{]+)\}.*')

    def path_constructor(loader, node):
        return os.path.expandvars(node.value)

    class EnvVarLoader(yaml.SafeLoader):
        pass

    EnvVarLoader.add_implicit_resolver('!path', path_matcher, None)
    EnvVarLoader.add_constructor('!path', path_constructor)

    with open(file) as f:
        c = yaml.load(f, Loader=EnvVarLoader)
        return(c)
