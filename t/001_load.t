use strict;
use warnings;

use Test::More tests => 1;

use Queue::Q;
use Queue::Q::NaiveFIFO;
use Queue::Q::NaiveDistFIFO;
use Queue::Q::ClaimFIFO;

use Queue::Q::NaiveFIFO::Redis;
use Queue::Q::NaiveDistFIFO::Redis;
use Queue::Q::ClaimFIFO::Redis;

pass("Alive");

