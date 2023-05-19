from xxdb.engine.capped_array import CappedArray


def test():
    test_size = 40

    arr = CappedArray(b'', test_size)
    arr.append(b'hello')
    arr.append(b'  okok')
    assert len(arr.dumps_data()) == 15

    arr2 = CappedArray(arr.dumps_data(), test_size)
    assert arr2.retrieve() == [b'hello', b'  okok']

    arr2.append(b'1' * 21)

    arr2.append(b'2' * 11)
    arr2.append(b'3' * 21)
    assert arr2.retrieve() == [b'22222222222', b'333333333333333333333']
