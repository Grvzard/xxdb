from xxdb.engine.capped_array import CappedArray


def ttest():
    print('[capped_array] test')

    data = b'\x00' * 40
    arr = CappedArray(data)
    arr.append(b'hello')
    arr.append(b'  okok')
    print(arr.dumps())

    print('--- arr-2 ---')
    arr2 = CappedArray(arr.dumps())
    print(arr2.retrive())
    arr2.append(b'1' * 21)
    print(arr2.retrive())
    arr2.append(b'2' * 11)
    arr2.append(b'3' * 21)
    print(arr2.retrive())
