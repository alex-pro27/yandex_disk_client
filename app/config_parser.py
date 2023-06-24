import re
from typing import Optional, TypeVar, Generic, Type, MutableMapping

from environs import Env
import yaml


class EnvDict:
    def __init__(self, env: Env, data: Optional[dict] = None, defaults: Optional[dict] = None):
        self._data = data or {}
        self._env = env
        if not isinstance(defaults, dict):
            defaults = {}
        self._defaults = defaults
        for k, v in self._data.items():
            if isinstance(v, dict):
                self._data[k] = self.as_env_dict(env, v, defaults.get(k))

    @classmethod
    def as_env_dict(cls, env, val, defaults=None):
        if isinstance(val, dict):
            new_env_dict = EnvDict(env, val, defaults)
            for k, v in new_env_dict.items():
                new_env_dict._data[k] = cls.as_env_dict(env, v)
            return new_env_dict
        return val

    def items(self):
        for k in self._data.keys():
            yield k, self.get(k)

    def keys(self):
        return self._data.keys()

    def values(self):
        for k in self._data.keys():
            yield self.get(k)

    def __getitem__(self, key):
        return self.get(key)

    def get(self, key, default=None):
        default = default or self._defaults
        value = self._data

        for s in key.split("."):
            if value is None:
                break
            if isinstance(default, (EnvDict, dict)):
                default = default.get(key)
            if isinstance(value, (EnvDict, dict)):
                value = value.get(s, default)

        if isinstance(value, str) and "${" in value:
            env_keys = re.findall(r"\${(.+)}", value)
            for env_key in env_keys:
                value = value.replace("${%s}" % env_key, self._env(env_key))
            return value or default
        return value


def get_config(conf_path: str, default_conf: Optional[dict] = None):
    env = Env()
    env.read_env()
    with open(conf_path) as stream:
        return EnvDict(env, yaml.safe_load(stream), default_conf)
