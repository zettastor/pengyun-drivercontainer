#!/usr/bin/perl
# Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 

use FindBin '$RealBin';
use File::Basename;
use File::Spec;
use Cwd;
# TODO: 
# 1. EnvironmentUtils can only be included when the script that calls it is run at the same directory
# as EnvironmentUtils does. 
# 2. Path::Class needs installed.
#
#
# get the the script absolute directory path
# We always assume that this script is under $env_root/bin
# so we can get the current script absolute directory path,
# and its parent dir is the environment root.

sub get_environment_alias() {
    # get the environment root first
    my $env_root = get_actual_environment_root();
    return basename($env_root);
}


sub get_actual_environment_root() {
    # get the script's directory
    my $myscript_dir = get_my_script_dir();
    return dirname($myscript_dir);
}

sub get_my_script_dir() {
    my $script_dir = File::Spec->catfile(cwd(), "bin");
    return $script_dir;
}
sub get_my_abs_script_dir() {
    return $RealBin;
}
1;
