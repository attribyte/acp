#!/bin/sh
VERSION="0.6.1"
cp acp-0.6.0.pom dist/lib/acp-${VERSION}.pom
cd dist/lib
gpg -ab acp-${VERSION}.pom
gpg -ab acp-${VERSION}.jar
gpg -ab acp-${VERSION}-sources.jar
gpg -ab acp-${VERSION}-javadoc.jar
jar -cvf ../bundle.jar *

