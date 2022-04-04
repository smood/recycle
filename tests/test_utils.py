from recycle.utils import fake_like_gen


class TestUtils:
    def test_fake_like_gen(self):

        anonymize = dict(field="str")
        gen = fake_like_gen(anonymize)

        run1 = gen()
        run2 = gen()
        assert list(run1.keys()) == ["field"]
        assert run1 != run2
