def cmp_version(v1, v2):
    """
    compare version (x.x.x), but only compare the major.minor part
    """
    v1 = v1.split('.')
    v2 = v2.split('.')
    for i in range(2):
        if int(v1[i]) > int(v2[i]):
            return 1
        elif int(v1[i]) < int(v2[i]):
            return -1
    return 0
