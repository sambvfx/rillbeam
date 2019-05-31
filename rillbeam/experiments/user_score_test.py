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

"""Test for the user_score example."""

from __future__ import absolute_import

import logging
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import user_score


class UserScoreTest(unittest.TestCase):

  SAMPLE_DATA = [
      'user3,team3,3,1447679463000,2015-11-16 13:11:03.921',
      'user4,team3,2,1447683063000,2015-11-16 14:11:03.921',
      'user1,team1,18,1447683063000,2015-11-16 14:11:03.921',
      'user1,team1,18,1447683063000,2015-11-16 14:11:03.921',
      # 2 hour gap
      'user2,team2,2,1447690263000,2015-11-16 16:11:03.955',
      # end user2
      'user3,team3,8,1447690263000,2015-11-16 16:11:03.955',
      'user3,team3,5,1447690263000,2015-11-16 16:11:03.959',
      # end user3
      'user4,team3,5,1447690263000,2015-11-16 16:11:03.959',
      # end user4
      'user1,team1,14,1447697463000,2015-11-16 18:11:03.955',
      # end user1
  ]

  def test_user_score(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(UserScoreTest.SAMPLE_DATA) | user_score.UserScore())
      assert_that(result, equal_to(
          [
              ('user1', [14, 18, 18]),
              ('user2', [2]),
              ('user3', [8, 5, 3]),
              ('user4', [5, 2])
          ]
      ))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
