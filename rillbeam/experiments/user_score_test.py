#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Test for the user_score example.
"""

from __future__ import absolute_import

import logging
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from rillbeam.experiments import user_score


class UserScoreTest(unittest.TestCase):

    SAMPLE_DATA = [
        'user3_team3,team3,3,1447679463000,2015-11-16 13:11:03.921',
        'user4_team3,team3,2,1447683063000,2015-11-16 14:11:03.921',
        'user1_team1,team1,18,1447683063000,2015-11-16 14:11:03.921',
        'user1_team1,team1,18,1447683063000,2015-11-16 14:11:03.921',
        # 2 hour gap
        'user2_team2,team2,2,1447690263000,2015-11-16 16:11:03.955',
        'user3_team3,team3,8,1447690263000,2015-11-16 16:11:03.955',
        'user3_team3,team3,5,1447690263000,2015-11-16 16:11:03.959',
        'user4_team3,team3,5,1447690263000,2015-11-16 16:11:03.959',
        'user1_team1,team1,14,1447697463000,2015-11-16 18:11:03.955',
    ]

    def test_user_score(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create(UserScoreTest.SAMPLE_DATA) | user_score.UserScore()
            )
            assert_that(result, equal_to(
                [
                  ('user1_team1', [14]),      # after gap
                  ('user1_team1', [18, 18]),  # before gap
                  ('user2_team2', [2]),       # after gap
                  ('user3_team3', [3]),       # before gap
                  ('user3_team3', [8, 5]),    # after gap
                  ('user4_team3', [2]),       # before gap
                  ('user4_team3', [5])        # after gap
                ]
            ))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
