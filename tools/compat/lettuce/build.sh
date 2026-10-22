#!/usr/bin/env bash
# Builds LettuceLoad without Maven or Gradle: downloads Lettuce 7.8.0 and
# the jars it needs (netty, reactor-core, slf4j-api) from Maven Central, checks them
# against the SHA-256 sums below, and compiles LettuceLoad.java with javac
# (JDK 17 or newer). Prints the class path to run it with.
#
#   tools/compat/lettuce/build.sh [DIR]     # default $LETTUCE_DIR or ~/.cache/speedvault/lettuce-7.8.0
#   java -cp "$(tools/compat/lettuce/build.sh)" LettuceLoad 127.0.0.1 7600 2000 default
#
# Downloads only what's missing; a jar whose sum is wrong is an error.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR="${1:-${LETTUCE_DIR:-$HOME/.cache/speedvault/lettuce-7.8.0}}"
mkdir -p "$DIR/jars" "$DIR/classes"
while read -r sum group artifact version; do
  jar="$DIR/jars/$artifact-$version.jar"
  if [ ! -f "$jar" ]; then
    curl -sSf -o "$jar.tmp" "https://repo1.maven.org/maven2/${group//.//}/$artifact/$version/$artifact-$version.jar"
    mv "$jar.tmp" "$jar"
  fi
  echo "$sum  $jar" | sha256sum --check --quiet >&2 || { rm -f "$jar"; echo "bad checksum: $jar" >&2; exit 1; }
done <<'EOF'
5b9b9f80bf04f20b1eb30b0f7f63dfd15024f4843d5a595a70e6cd5bfbfe791e io.lettuce lettuce-core 7.8.0.RELEASE
0d249178ec0204b35a6ad3e5ac8a6c2ecb3eb496c6cdae40b8eca4fa3cb6a0c7 io.netty netty-buffer 4.2.17.Final
3b92bc3b7d231fa6f4889b2b9281cae6b634428b8121fb1b9b5fccba1543d352 io.netty netty-codec-base 4.2.17.Final
394f4649a824683735f28b299d0a48caff059ae30e654e2abf8b3f392af4d374 io.netty netty-codec-dns 4.2.17.Final
502aae2a6680e9bca3558a3fa098f06dc4d87340b8b714e925a6d690871c092a io.netty netty-common 4.2.17.Final
4df11c7520b556c5e2c84b939182699d48fcbb09e97a6bb5e1c0b6835227d126 io.netty netty-handler 4.2.17.Final
7a4b599b9c0c29a505d3b7311f142df883fb27d0f67495a7cbaf533b0e456623 io.netty netty-resolver 4.2.17.Final
d332481c22136a68c2ee9e45e86212174932587d93a5dd4d201e196828bf9f91 io.netty netty-resolver-dns 4.2.17.Final
d2409ce3a735cbab4188d0ea5d82abed823a6a07a8389f49af4a9e1243a562bc io.netty netty-transport 4.2.17.Final
e01aa467b9104de54a57c08c290f70b22e53ec97cf5583c87c112170ef5832cc io.netty netty-transport-native-unix-common 4.2.17.Final
9a5f1bfc5ad0416a410ff63beaa279cc30c2da3ae9b111a678c83c99298a1551 io.projectreactor reactor-core 3.8.7
f75ca597789b3dac58f61857b9ac2e1034a68fa672db35055a8fb4509e325f28 org.reactivestreams reactive-streams 1.0.4
d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0 org.slf4j slf4j-api 1.7.36
EOF
if [ ! -f "$DIR/classes/LettuceLoad.class" ] || [ "$HERE/LettuceLoad.java" -nt "$DIR/classes/LettuceLoad.class" ]; then
  javac --release 17 -nowarn -cp "$DIR/jars/*" -d "$DIR/classes" "$HERE/LettuceLoad.java" >&2
fi
echo "$DIR/jars/*:$DIR/classes"
