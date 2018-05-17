/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.time;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.time.Duration;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractLongAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class PerProfileTimestampGeneratorIT {

  @Rule public CcmRule ccmRule = CcmRule.getInstance();

  @Rule
  public SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withOptions(
              "request.timestamp-generator {",
              String.format("  class = \"%s\"", ConstantTimestampGenerator.class.getName()),
              "  value = 100",
              "  foo = 100",
              "}",
              "profiles.profile1.request.timestamp-generator {",
              String.format("  class = \"%s\"", ConstantTimestampGenerator.class.getName()),
              "  value = 101",
              "}",
              "profiles.profile2 {}")
          .build();

  @Before
  public void setup() {
    CqlSession session = sessionRule.session();
    session.execute("CREATE TABLE IF NOT EXISTS foo(k int primary key, v int)");

    // Sanity checks
    DriverContext context = session.getContext();
    DriverConfig config = context.config();
    assertThat(config.getNamedProfiles()).containsKeys("profile1", "profile2");

    assertThat(context.timestampGenerators())
        .hasSize(3)
        .containsKeys(DriverConfigProfile.DEFAULT_NAME, "profile1", "profile2");
    TimestampGenerator defaultGenerator =
        context.timestampGenerators().get(DriverConfigProfile.DEFAULT_NAME);
    TimestampGenerator generator1 = context.timestampGenerators().get("profile1");
    TimestampGenerator generator2 = context.timestampGenerators().get("profile2");
    assertThat(defaultGenerator)
        .isInstanceOf(ConstantTimestampGenerator.class)
        .isSameAs(generator2)
        .isNotSameAs(generator1);
  }

  @Test
  public void should_use_timestamp_generator_from_request_profile() {
    assertThatTimestampForProfileName(null).isEqualTo(100);
    // overrides the generator:
    assertThatTimestampForProfileName("profile1").isEqualTo(101);
    // inherits from the default profile:
    assertThatTimestampForProfileName("profile2").isEqualTo(100);

    DriverConfig config = sessionRule.session().getContext().config();

    // Same as above, but with the profile instances
    assertThatTimestampForProfile(config.getDefaultProfile()).isEqualTo(100);
    assertThatTimestampForProfile(config.getNamedProfile("profile1")).isEqualTo(101);
    assertThatTimestampForProfile(config.getNamedProfile("profile2")).isEqualTo(100);

    // Derived profiles should inherit their parent's generator
    DriverConfigProfile derivedProfile =
        config
            .getDefaultProfile()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10));
    assertThatTimestampForProfile(derivedProfile).isEqualTo(100);
  }

  private AbstractLongAssert<?> assertThatTimestampForProfileName(String profileName) {
    return assertThatTimestampFor(builder -> builder.withConfigProfileName(profileName));
  }

  private AbstractLongAssert<?> assertThatTimestampForProfile(DriverConfigProfile profile) {
    return assertThatTimestampFor(builder -> builder.withConfigProfile(profile));
  }

  private AbstractLongAssert<?> assertThatTimestampFor(
      Consumer<SimpleStatementBuilder> setProfile) {
    CqlSession session = sessionRule.session();

    session.execute("TRUNCATE foo");
    SimpleStatementBuilder builder = SimpleStatement.builder("INSERT INTO foo (k,v) VALUES (1,1)");
    setProfile.accept(builder);
    session.execute(builder.build());

    long actualTimestamp =
        session.execute("SELECT writetime(v) FROM foo WHERE k = 1").one().getLong(0);
    return assertThat(actualTimestamp);
  }

  /** A (stupid) timestamp generator that always returns the same configurable value. */
  public static class ConstantTimestampGenerator implements TimestampGenerator {

    private final int value;

    public ConstantTimestampGenerator(DriverContext context, String profileName) {
      DriverConfigProfile config = context.config().getNamedProfile(profileName);
      this.value = config.getInt(VALUE);
    }

    @Override
    public long next() {
      return value;
    }

    private static final DriverOption VALUE =
        new DriverOption() {
          @Override
          public String getPath() {
            return "request.timestamp-generator.value";
          }

          @Override
          public boolean required() {
            return false;
          }
        };
  }
}
