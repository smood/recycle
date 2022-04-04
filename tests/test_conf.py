import dataconf
from recycle import config


class TestConf:
    def test_conf(self):

        conf = dataconf.load("./ops/mongo.hocon", config.Ops)
        assert len(conf.transforms["smoodapiv2"].keys()) == 136
