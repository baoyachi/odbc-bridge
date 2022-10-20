# how to build docker develop env

## dameng odbc driver

### check *.so file dependencies
* E.g: check `libdodbc.so`
```bash
ldd libdodbc.so
```
> output
```bash
root@d4229423fe25:/usr/lib/odbc# ldd libdodbc.so
        linux-vdso.so.1 (0x00007ffcf2ef8000)
        libdmdpi.so => not found
        libdmfldr.so => not found
        libdmelog.so => not found
        libdmutl.so => not found
        libdmclientlex.so => not found
        libdmos.so => not found
        libdmcvt.so => not found
        libdmstrt.so => not found
        librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x00007fbfde2bd000)
        libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007fbfde2b8000)
        libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007fbfde2b3000)
        libstdc++.so.6 => /lib/x86_64-linux-gnu/libstdc++.so.6 (0x00007fbfdddd6000)
        libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007fbfddcef000)
        libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007fbfddac7000)
        libgcc_s.so.1 => /lib/x86_64-linux-gnu/libgcc_s.so.1 (0x00007fbfde291000)
        /lib64/ld-linux-x86-64.so.2 (0x00007fbfde2c8000)
root@d4229423fe25:/usr/lib/odbc# 
```

