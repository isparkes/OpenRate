#!/usr/bin/env bash
#
# Build OpenRate and install it into the local Maven repository.
#
# OpenRate is not published to Maven Central, so applications built on it need
# the framework installed locally before they can resolve the dependency.
# Run this once, then build the application you are interested in.

set -euo pipefail

cd "$(dirname "$0")"

REQUIRED_JAVA=17

say()  { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- java check
command -v java >/dev/null 2>&1 || fail \
  "No 'java' on the PATH. OpenRate needs JDK ${REQUIRED_JAVA} or later.
       macOS:  brew install --cask temurin@${REQUIRED_JAVA}
       Linux:  apt install openjdk-${REQUIRED_JAVA}-jdk   (or your package manager)"

# "openjdk version \"17.0.21\"" -> 17 ; "java version \"1.8.0_401\"" -> 8
raw_version=$(java -version 2>&1 | head -1 | sed -n 's/.*version "\([^"]*\)".*/\1/p')
case "$raw_version" in
  1.*) java_major=$(printf '%s' "$raw_version" | cut -d. -f2) ;;
  *)   java_major=$(printf '%s' "$raw_version" | cut -d. -f1) ;;
esac

[ -n "$java_major" ] || fail "Could not determine the Java version from: $raw_version"

if [ "$java_major" -lt "$REQUIRED_JAVA" ]; then
  fail "Java ${raw_version} found, but OpenRate needs ${REQUIRED_JAVA} or later.
       macOS:  brew install --cask temurin@${REQUIRED_JAVA}
       then:   export JAVA_HOME=\$(/usr/libexec/java_home -v ${REQUIRED_JAVA})"
fi

# --------------------------------------------------------------- maven check
command -v mvn >/dev/null 2>&1 || fail \
  "No 'mvn' on the PATH. OpenRate is built with Maven.
       macOS:  brew install maven
       Linux:  apt install maven   (or your package manager)"

say "Java ${raw_version}, $(mvn -v 2>/dev/null | head -1)"
say ""

# --------------------------------------------------------------------- build
say "Building and installing OpenRate (this runs the full test suite)..."
say ""

# A hung concurrency test should fail the build rather than hang it
if mvn -B clean install -Dsurefire.timeout=600; then
  version=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version 2>/dev/null | tail -1)
  say ""
  say "--------------------------------------------------------------"
  say " OpenRate ${version} installed to your local Maven repository"
  say "--------------------------------------------------------------"
  say ""
  say "Next: build and run an application. The simplest one is"
  say "OpenRateSimple, which needs no database and no extra setup:"
  say ""
  say "    git clone https://github.com/isparkes/OpenRateSimple.git"
  say "    cd OpenRateSimple"
  say "    ./demo.sh"
  say ""
else
  say ""
  fail "The build failed. See the Maven output above."
fi
