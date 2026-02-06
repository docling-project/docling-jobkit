## [v1.10.1](https://github.com/docling-project/docling-jobkit/releases/tag/v1.10.1) - 2026-02-06

### Fix

* Docs typo ([#88](https://github.com/docling-project/docling-jobkit/issues/88)) ([`35ed93d`](https://github.com/docling-project/docling-jobkit/commit/35ed93d724f2a8f6a094ec9d98ea5788f7f9e2a7))

## [v1.10.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.10.0) - 2026-02-05

### Feature

* Add charts extraction ([#87](https://github.com/docling-project/docling-jobkit/issues/87)) ([`605d6ee`](https://github.com/docling-project/docling-jobkit/commit/605d6ee4e90d2ec6a2b0b51c0cd83aafeeb2b6e5))
* Implement clear_cache_task in RQ worker and invoke it on clear_converters ([#86](https://github.com/docling-project/docling-jobkit/issues/86)) ([`ded0ea3`](https://github.com/docling-project/docling-jobkit/commit/ded0ea396cf262d6c25b77efc3df588e49492122))

## [v1.9.1](https://github.com/docling-project/docling-jobkit/releases/tag/v1.9.1) - 2026-01-30

### Fix

* Disable un-requested parquet generation ([#85](https://github.com/docling-project/docling-jobkit/issues/85)) ([`dcee4ad`](https://github.com/docling-project/docling-jobkit/commit/dcee4ad86947327422eea3e65a2756d0e0b95fbe))

## [v1.9.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.9.0) - 2026-01-28

### Feature

* Python 3.14 support ([#71](https://github.com/docling-project/docling-jobkit/issues/71)) ([`5e25748`](https://github.com/docling-project/docling-jobkit/commit/5e257484a2a9a5f3002abe64c4e73600702cf7a4))
* Add local filesystem path processors for source and target ([#81](https://github.com/docling-project/docling-jobkit/issues/81)) ([`750b496`](https://github.com/docling-project/docling-jobkit/commit/750b496da180b70d607114cd899819afbbafc8bb))
* Docling-jobkit-multiproc cli running parallel subprocesses ([#80](https://github.com/docling-project/docling-jobkit/issues/80)) ([`ad08d41`](https://github.com/docling-project/docling-jobkit/commit/ad08d41a327bfe9063ff32bf57ed20fd3627cee7))
* Batching Support for Source Processors ([#79](https://github.com/docling-project/docling-jobkit/issues/79)) ([`f109e53`](https://github.com/docling-project/docling-jobkit/commit/f109e5362b9714655e862bbac29e6b6f2c1902a3))

### Fix

* Set result_ttl for RQ ([#83](https://github.com/docling-project/docling-jobkit/issues/83)) ([`e0d4b9a`](https://github.com/docling-project/docling-jobkit/commit/e0d4b9afe112939dffe9c2b7f558196195761507))
* Memory leak fixes in RQ and Local engines ([#82](https://github.com/docling-project/docling-jobkit/issues/82)) ([`f3c74d7`](https://github.com/docling-project/docling-jobkit/commit/f3c74d74aefd11bea0b8e11c5fec3512564e3219))

## [v1.8.1](https://github.com/docling-project/docling-jobkit/releases/tag/v1.8.1) - 2026-01-06

### Fix

* Isolate VLM pipeline options to avoid cache collisions ([#73](https://github.com/docling-project/docling-jobkit/issues/73)) ([`eaea195`](https://github.com/docling-project/docling-jobkit/commit/eaea1954170e4633f6d7e05048e3fe139b3157ca))

## [v1.8.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.8.0) - 2025-10-31

### Feature

* Expose new standard pipeline with threads and its parameters ([#70](https://github.com/docling-project/docling-jobkit/issues/70)) ([`b72f7f8`](https://github.com/docling-project/docling-jobkit/commit/b72f7f857938eb71d3a2a03419d95bc9bb2cbbe4))

## [v1.7.1](https://github.com/docling-project/docling-jobkit/releases/tag/v1.7.1) - 2025-10-30

### Fix

* Missing temperature parameter for vlm pipeline models ([#69](https://github.com/docling-project/docling-jobkit/issues/69)) ([`5b1914c`](https://github.com/docling-project/docling-jobkit/commit/5b1914c91c620681af75fa1d00e3cc230db5bfc4))

## [v1.7.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.7.0) - 2025-10-21

### Feature

* Use new docling auto-ocr ([#64](https://github.com/docling-project/docling-jobkit/issues/64)) ([`6d7a4d2`](https://github.com/docling-project/docling-jobkit/commit/6d7a4d23a532bb64397fb96559bcd8715ba3da78))

### Documentation

* Param typo ([#67](https://github.com/docling-project/docling-jobkit/issues/67)) ([`448abc2`](https://github.com/docling-project/docling-jobkit/commit/448abc29bba0c5eee09387c0621b40d2543c66f4))

## [v1.6.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.6.0) - 2025-10-03

### Feature

* Create connectors to import/export documents from/to Google Drive ([#62](https://github.com/docling-project/docling-jobkit/issues/62)) ([`08cf076`](https://github.com/docling-project/docling-jobkit/commit/08cf0768e995652620fe905de9803be7bcf6d7a9))
* **docling:** Update docling version with support for GraniteDocling ([#63](https://github.com/docling-project/docling-jobkit/issues/63)) ([`291b757`](https://github.com/docling-project/docling-jobkit/commit/291b757f50f92ae3da10facb50d6a967836ba583))
* Kubeflow pipeline using docling with remote inference server ([#57](https://github.com/docling-project/docling-jobkit/issues/57)) ([`2188b96`](https://github.com/docling-project/docling-jobkit/commit/2188b9699ac72c9ed6492a86eed0b619e5b5320a))

### Documentation

* Fix description of default table structure mode ([#58](https://github.com/docling-project/docling-jobkit/issues/58)) ([`0d88e9e`](https://github.com/docling-project/docling-jobkit/commit/0d88e9e36bb8406a9e0caa6eafd2dfe06576bdac))

## [v1.5.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.5.0) - 2025-09-08

### Feature

* Add chunking task ([#54](https://github.com/docling-project/docling-jobkit/issues/54)) ([`3b9b11c`](https://github.com/docling-project/docling-jobkit/commit/3b9b11cf9fc636da1cd8d4de89b59cf9e7b09d04))

### Fix

* Fixes name cleaning of doc on s3 for batching ([#55](https://github.com/docling-project/docling-jobkit/issues/55)) ([`9b7276c`](https://github.com/docling-project/docling-jobkit/commit/9b7276c1d5c69dd99dd5d0d4362ab718136a2dc5))
* Fix for parquet file generation with s3 connector and temporary storage ([#52](https://github.com/docling-project/docling-jobkit/issues/52)) ([`1180c07`](https://github.com/docling-project/docling-jobkit/commit/1180c07f41b731fa8b29d482c367fa72b7933f25))
* Fixing s3_connector scratch directory ([#51](https://github.com/docling-project/docling-jobkit/issues/51)) ([`74710d0`](https://github.com/docling-project/docling-jobkit/commit/74710d065cfebea60d00a6ef1305f4a397d294a1))

## [v1.4.1](https://github.com/docling-project/docling-jobkit/releases/tag/v1.4.1) - 2025-08-19

### Fix

* Propagate allow_external_plugins ([#50](https://github.com/docling-project/docling-jobkit/issues/50)) ([`46653a3`](https://github.com/docling-project/docling-jobkit/commit/46653a3fd60cfbe6baff2ed3a7ccc1d44dae39b4))

## [v1.4.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.4.0) - 2025-08-13

### Feature

* Add rq orchestrator ([#44](https://github.com/docling-project/docling-jobkit/issues/44)) ([`d7b1c40`](https://github.com/docling-project/docling-jobkit/commit/d7b1c40943303c25bcbf99b70a3cb9f93ed41165))

## [v1.3.1](https://github.com/docling-project/docling-jobkit/releases/tag/v1.3.1) - 2025-08-12

### Fix

* Selection logic for vlm providers ([#49](https://github.com/docling-project/docling-jobkit/issues/49)) ([`9054df1`](https://github.com/docling-project/docling-jobkit/commit/9054df1f327d5d61955baf903890adbffa3cbc0e))

## [v1.3.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.3.0) - 2025-08-06

### Feature

* Vlm models params ([#48](https://github.com/docling-project/docling-jobkit/issues/48)) ([`5b83c13`](https://github.com/docling-project/docling-jobkit/commit/5b83c13de74ef1a3e917c031716e9e242a2d276c))
* Expose table cell matching parameter ([#47](https://github.com/docling-project/docling-jobkit/issues/47)) ([`961c286`](https://github.com/docling-project/docling-jobkit/commit/961c286ee086ae3f739d8ca307bfe5fd39689489))
* Option to disable shared models between workers ([#46](https://github.com/docling-project/docling-jobkit/issues/46)) ([`7d652a5`](https://github.com/docling-project/docling-jobkit/commit/7d652a53df21606c9c94c718583a636689048919))

## [v1.2.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.2.0) - 2025-07-24

### Feature

* Add new task source and targets ([#42](https://github.com/docling-project/docling-jobkit/issues/42)) ([`b001914`](https://github.com/docling-project/docling-jobkit/commit/b00191407cf77444d3e0827e44c93a88c6dedaa5))

## [v1.1.1](https://github.com/docling-project/docling-jobkit/releases/tag/v1.1.1) - 2025-07-18

### Fix

* Thread-safe cache of converter options ([#43](https://github.com/docling-project/docling-jobkit/issues/43)) ([`cf12e47`](https://github.com/docling-project/docling-jobkit/commit/cf12e4795dba3184d59c2f513a70aa30a28eeacc))

## [v1.1.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.1.0) - 2025-07-14

### Feature

* Add task target options for docling-serve v1 ([#41](https://github.com/docling-project/docling-jobkit/issues/41)) ([`2633c96`](https://github.com/docling-project/docling-jobkit/commit/2633c96d363540858c7d775ed76206dda309426c))

## [v1.0.0](https://github.com/docling-project/docling-jobkit/releases/tag/v1.0.0) - 2025-07-07

### Feature

* Add and refactor orchestrator engines used in docling-serve ([#39](https://github.com/docling-project/docling-jobkit/issues/39)) ([`b9257ac`](https://github.com/docling-project/docling-jobkit/commit/b9257ac1afea9ddb2674c845ff680c3afa0e5f3e))

### Breaking

* Add and refactor orchestrator engines used in docling-serve ([#39](https://github.com/docling-project/docling-jobkit/issues/39)) ([`b9257ac`](https://github.com/docling-project/docling-jobkit/commit/b9257ac1afea9ddb2674c845ff680c3afa0e5f3e))

## [v0.2.0](https://github.com/docling-project/docling-jobkit/releases/tag/v0.2.0) - 2025-06-25

### Feature

* Add upload parquet and manifest files ([#25](https://github.com/docling-project/docling-jobkit/issues/25)) ([`ab7c04a`](https://github.com/docling-project/docling-jobkit/commit/ab7c04a908d68743c135913cf069041a3f9acb2b))

### Documentation

* How to run kfp pipeline manually ([#36](https://github.com/docling-project/docling-jobkit/issues/36)) ([`0a3b6d4`](https://github.com/docling-project/docling-jobkit/commit/0a3b6d491e93188a60ee4e71d0247eefe781bf2c))

## [v0.1.0](https://github.com/docling-project/docling-jobkit/releases/tag/v0.1.0) - 2025-05-13

### Feature

* Implements fix for the issue caused by passing batch list of objects are regular parameter ([#31](https://github.com/docling-project/docling-jobkit/issues/31)) ([`3f5e8b3`](https://github.com/docling-project/docling-jobkit/commit/3f5e8b3a76d35902bd558d1d10c3a2e66320a616))

### Fix

* Convert document exception handler ([#34](https://github.com/docling-project/docling-jobkit/issues/34)) ([`2c27c71`](https://github.com/docling-project/docling-jobkit/commit/2c27c71b75da98f04fccc7abc7ddc3a9a3afb0cd))
* Pinning of the new base image ([#32](https://github.com/docling-project/docling-jobkit/issues/32)) ([`1e068ea`](https://github.com/docling-project/docling-jobkit/commit/1e068ea8804e96bfe222906787d411b97743237e))
* Wrong indentation in convert_documents method ([#29](https://github.com/docling-project/docling-jobkit/issues/29)) ([`27bad5b`](https://github.com/docling-project/docling-jobkit/commit/27bad5b9159bd0fcb7c84be940416c6738c03b86))
* Add missing doc max size ([#27](https://github.com/docling-project/docling-jobkit/issues/27)) ([`89dd116`](https://github.com/docling-project/docling-jobkit/commit/89dd1169fe7a965a09f91b7e2ef4ceecb1236e71))
* Export pdf, check existing conversions ([#26](https://github.com/docling-project/docling-jobkit/issues/26)) ([`3e55ce9`](https://github.com/docling-project/docling-jobkit/commit/3e55ce999a07032f26c150c4d6a9080e22edc1f3))

## [v0.0.2](https://github.com/docling-project/docling-jobkit/releases/tag/v0.0.2) - 2025-04-16

### Fix

* CI and formatting ([#18](https://github.com/docling-project/docling-jobkit/issues/18)) ([`ca15f5f`](https://github.com/docling-project/docling-jobkit/commit/ca15f5f25632297efd05198d10ba19b5312d6b49))
