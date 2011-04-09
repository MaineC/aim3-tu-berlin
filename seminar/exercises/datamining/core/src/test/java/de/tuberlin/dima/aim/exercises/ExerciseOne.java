/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.dima.aim.exercises;

import de.tuberlin.dima.aim.exercises.one.FilteringWordCountTest;
import de.tuberlin.dima.aim.exercises.one.PrimeNumbersWritableTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * mvn -Dtest=ExerciseOne clean test -Pcheck
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ FilteringWordCountTest.class, PrimeNumbersWritableTest.class})
public class ExerciseOne {}
