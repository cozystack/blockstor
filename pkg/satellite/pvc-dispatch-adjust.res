resource pvc-dispatch-adjust {
  protocol C;
  on n1 {
    address 10.0.0.1:7000;
    node-id 0;
    volume 0 {
      device /dev/drbd1000 minor 1000;
      disk /dev/vg/pvc-dispatch-adjust_00000;
      meta-disk internal;
    }
  }
  on n2 {
    address 10.0.0.2:7000;
    node-id 1;
    volume 0 {
      device /dev/drbd1000 minor 1000;
      disk /dev/drbd/this/is/not/used;
      meta-disk internal;
    }
  }
  connection {
    host n1 address 10.0.0.1:7000;
    host n2 address 10.0.0.2:7000;
  }
}
