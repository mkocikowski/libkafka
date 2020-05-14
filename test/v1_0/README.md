This is a separate environment that, in addition to the 2.3 kafka on 9092, also spins up 1.0 on
9093. This is used to test "by hand" the temporary support for producing to kafka 1.0. This env
needs to be up for running tests with `-tags=v1_0`.

This is not in the CI. This is a hack that will go away (cough).
