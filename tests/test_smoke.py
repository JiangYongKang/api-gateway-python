"""骨架冒烟测试：验证包可导入、应用可创建。"""


def test_imports() -> None:
    import gateway.app  # noqa: F401
    import gateway.config  # noqa: F401
    import gateway.errors  # noqa: F401
