nsq_sketch
==========

nsq_sketch - a top-n view of data in a [NSQ](https://github.com/bitly/nsq) stream of json messages

---

Given a stream of json messages in NSQ like `{"c": "US"}` nsq_sketch uses [countmin](https://github.com/jehiah/countmin) to very efficiently track the top-n occurances of values

```
$ nsq_sketch --lookupd-http-address=$LOOKUPD --topic=... --interval=1s --key=c
2013/05/15 19:29:38 Sketching for 2m0s intervals. Displaying 15 top items every 1s

----- 1334 messages ----
438 - US
171 - GB
84 - BR
65 - CA
42 - DE
41 - TR
38 - IT
34 - FR
26 - MX
22 - AR
21 - RO
20 - HK
19 - ES
17 - VN
```