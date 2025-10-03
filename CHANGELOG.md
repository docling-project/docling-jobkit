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
