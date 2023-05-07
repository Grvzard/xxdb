from xxdb.engine.capped_array import CappedArray


def test():
    test_size = 40
    data = b'\x00' * test_size

    arr = CappedArray(data)
    arr.append(b'hello')
    arr.append(b'  okok')
    assert len(arr.dumps()) == test_size

    arr2 = CappedArray(arr.dumps())
    assert arr2.retrieve() == [b'hello', b'  okok']

    arr2.append(b'1' * 21)

    arr2.append(b'2' * 11)
    arr2.append(b'3' * 21)
    assert arr2.retrieve() == [b'22222222222', b'333333333333333333333']
