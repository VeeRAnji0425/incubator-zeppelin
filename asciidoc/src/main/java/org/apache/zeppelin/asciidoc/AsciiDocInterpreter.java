/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.asciidoc;

import static org.asciidoctor.Asciidoctor.Factory.create;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.asciidoctor.Asciidoctor;

/**
 * An interpreter for AsciiDoc
 */
public class AsciiDocInterpreter extends Interpreter {

  private Asciidoctor asciidoctor;

  static {
    Interpreter.register("asciidoc", AsciiDocInterpreter.class.getName());
  }

  public AsciiDocInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    asciidoctor = create();
  }

  @Override
  public void close() {
    asciidoctor.shutdown();
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    final String htmlOutput = asciidoctor.convert(st, new HashMap<String, Object>());
    return new InterpreterResult(Code.SUCCESS, Type.HTML, htmlOutput);
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return new ArrayList<>();
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
            AsciiDocInterpreter.class.getName() + this.hashCode(), 10);
  }
}
