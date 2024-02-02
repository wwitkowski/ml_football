# pylint: skip-file
from etl.transform import TransformPipeline


def test_add_operation():
    pipe = TransformPipeline()
    operation = lambda x: [i + 1 for i in x]
    pipe.add_operation(operation)
    assert pipe._operations[0] == (operation, (), {})
    assert len(pipe._operations) == 1

    pipe.add_operation(lambda x: x * 2)
    assert len(pipe._operations) == 2


def test_apply_one_op():
    pipe = TransformPipeline()
    pipe.add_operation(lambda x: [i + 1 for i in x])
    data = [1, 2, 3]
    assert pipe.apply(data) == [2, 3, 4]


def test_apply_two_ops():
    pipe = TransformPipeline()
    pipe.add_operation(lambda x: [i + 1 for i in x])
    pipe.add_operation(lambda x: [i * 2 for i in x])
    data = [1, 2, 3]
    assert pipe.apply(data) == [4, 6, 8]


def test_copy():
    base_pipe = TransformPipeline()
    base_pipe.add_operation(lambda x: [i + 1 for i in x])
    other_pipe = base_pipe.copy()

    data = [5, 10, 15]

    assert len(base_pipe._operations) == len(other_pipe._operations)
    assert base_pipe.apply(data) == other_pipe.apply(data)


def test_branch_pipelines():
    base_pipe = TransformPipeline()
    base_pipe.add_operation(lambda x: [i + 1 for i in x])
    other_pipe = base_pipe.copy()
    other_pipe.add_operation(lambda x: [i * 2 for i in x])

    data = [5, 10, 15]

    assert len(base_pipe._operations) == 1
    assert len(other_pipe._operations) == 2
    assert base_pipe.apply(data) == [6, 11, 16]
    assert other_pipe.apply(data) == [12, 22, 32]
