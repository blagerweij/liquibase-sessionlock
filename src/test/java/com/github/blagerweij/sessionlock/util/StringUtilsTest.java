package com.github.blagerweij.sessionlock.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StringUtilsTest {
    @Test
    void constructor() {
        assertThatThrownBy(() -> new StringUtils()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void truncate() {
        assertThat(StringUtils.truncate("testing", 4)).isEqualTo("test");
        assertThat(StringUtils.truncate("test", 4)).isEqualTo("test");
        assertThat(StringUtils.truncate("t", 4)).isEqualTo("t");
        assertThat(StringUtils.truncate(null, 4)).isEqualTo(null);
    }

    @Test
    void toUpperCase() {
        assertThat(StringUtils.toUpperCase("testing")).isEqualTo("TESTING");
        assertThat(StringUtils.toUpperCase(null)).isEqualTo(null);
    }
}
