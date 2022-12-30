#!/usr/bin/perl
#
# 010LoadMod
#
# author zjm
#
# This script replaces original nbd kernel module with our nbd module,
# and backup original one to "nbd.ko.ori".
#
# Note: this script must be ran in parent dir of the script path,
# which is usually "bin" directory in our system.
#

use strict;
use warnings;
use FindBin '$RealBin';
use File::Spec;

require("$RealBin/EnvironmentUtils.pm");

my $linux_kernel_version = `uname -r`;
chomp($linux_kernel_version);

my $my_script_dir = get_my_script_dir();
my $nbd_modules_dir = File::Spec->catfile($my_script_dir, "nbd_modules");
my $target_nbd_module_path = undef;

opendir(DIR, $nbd_modules_dir) or die $!;
my @candi_ver_list = readdir(DIR);
closedir DIR;

# pick proper module which fits local linux kernel version from our modules
for my $candi_ver (@candi_ver_list) {
    # skip linux cur dir and parent dir with symbol "." and ".."
    next if ($candi_ver =~ /^\.\.?$/);

    if ($linux_kernel_version =~ $candi_ver) {
        $target_nbd_module_path = File::Spec->catfile($nbd_modules_dir, "$candi_ver/py_nbd.ko");
        last;
    }
}

die("unsupport nbd module in current linux version $linux_kernel_version") unless $target_nbd_module_path;

my $nbd_module_dir_in_linux = "/lib/modules/$linux_kernel_version/kernel/drivers/block/";
my $nbd_module_path_in_linux = File::Spec->catfile($nbd_module_dir_in_linux, "py_nbd.ko");

# load our nbd module
system("cp $target_nbd_module_path $nbd_module_path_in_linux");
system("depmod -a; modprobe -r nbd; modprobe -r py_nbd; modprobe py_nbd");

die "unable to load module nbd" if system("lsmod | grep nbd");
