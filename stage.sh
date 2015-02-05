#!/bin/sh
cp acp-0.6.0.pom dist/lib
cd dist/lib
gpg -ab acp-0.6.0.pom
gpg -ab acp-0.6.0.jar
gpg -ab acp-0.6.0-sources.jar
gpg -ab acp-0.6.0-javadoc.jar
jar -cvf ../bundle.jar *

