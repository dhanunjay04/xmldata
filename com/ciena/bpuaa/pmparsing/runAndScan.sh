#!/bin/bash
# shellcheck disable=SC2006
# shellcheck disable=SC2126
result=`ps aux | grep -i "nokiaNspPmFileParser.py" | grep -v "grep" | wc -l`
if [ $result -ge 1 ]
   then
        echo "script is running"
   else
        echo "script is not running"
        /Users/dvarakal/workspace/others/special/docomo/pm-response/xmldata/python3 nokiaNspPmFileParser.py &
fi