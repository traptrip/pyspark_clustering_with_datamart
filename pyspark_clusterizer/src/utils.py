from omegaconf import OmegaConf


def read_config(cfg_path: str):
    return OmegaConf.load(cfg_path)
