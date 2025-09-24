# Changelog

## [0.7.0](https://github.com/bihealth/cubi-tk/compare/v0.6.0...v0.7.0) (2025-09-24)


### Features

* add `cubi-tk sodar deletion-requests` ([#297](https://github.com/bihealth/cubi-tk/issues/297)) ([8edec81](https://github.com/bihealth/cubi-tk/commit/8edec813b9041b62e89c98e9b9950d9c5c0e96d3))
* changes for onko data upload ([#289](https://github.com/bihealth/cubi-tk/issues/289)) ([f20401c](https://github.com/bihealth/cubi-tk/commit/f20401c7822da0226f045f0c7145678f8f58fcc0))
* introduce and refactor SodarIngestBase incl harmonised options ([#295](https://github.com/bihealth/cubi-tk/issues/295)) ([6d15d9b](https://github.com/bihealth/cubi-tk/commit/6d15d9b9630515d93c5c42396c94d8a275add0b4))
* major cubi-tk refactoring ([#283](https://github.com/bihealth/cubi-tk/issues/283)) ([1a5397d](https://github.com/bihealth/cubi-tk/commit/1a5397dc04f890b2cbf74666e6c1c3731ae25158))
* update docs ([#309](https://github.com/bihealth/cubi-tk/issues/309)) ([5f34d4f](https://github.com/bihealth/cubi-tk/commit/5f34d4f33962fbc8bca5ae8c009d75fb7ecd7ce4))
* update ingest-data for wgs germline ([#301](https://github.com/bihealth/cubi-tk/issues/301)) ([3316e9b](https://github.com/bihealth/cubi-tk/commit/3316e9b56f550deb3db1d4f4b1931aa8b458cffa))


### Bug Fixes

* (Sodar)API POST calls need to use the data field, not params ([#288](https://github.com/bihealth/cubi-tk/issues/288)) ([ce7388b](https://github.com/bihealth/cubi-tk/commit/ce7388bc9d333734da896c7d066e05032c9acaae))
* add defaults for --output-directory in snappy pull-data commands ([#302](https://github.com/bihealth/cubi-tk/issues/302)) ([e581861](https://github.com/bihealth/cubi-tk/commit/e581861b1de69f1c638bf78dc31c87bb0e1e5dc8))
* bug fix val in feat removed thats needed ([#290](https://github.com/bihealth/cubi-tk/issues/290)) ([971c12f](https://github.com/bihealth/cubi-tk/commit/971c12f00a392782d1103236f4541a3bad95c72b))
* call altamisa with typer, fixes [#249](https://github.com/bihealth/cubi-tk/issues/249) ([#279](https://github.com/bihealth/cubi-tk/issues/279)) ([70f8667](https://github.com/bihealth/cubi-tk/commit/70f8667e5d8f07a6394d3c8ea24b68bdec4845ef))
* dict needs items keyword ([#287](https://github.com/bihealth/cubi-tk/issues/287)) ([fa2333d](https://github.com/bihealth/cubi-tk/commit/fa2333d095ee55b322d1bae9a73f6bf8277d649b))
* dont gen checksum for checksums ([#293](https://github.com/bihealth/cubi-tk/issues/293)) ([1d8fe5b](https://github.com/bihealth/cubi-tk/commit/1d8fe5bb8b175a6ea42f213f44d768607d3c94fd))
* get assay UUID pull data ([#285](https://github.com/bihealth/cubi-tk/issues/285)) ([f121236](https://github.com/bihealth/cubi-tk/commit/f121236c61c3238c57da58350fff8ae0abec96ad))
* nicer json output for LZ commands, always write this json to stdout, better test ([#303](https://github.com/bihealth/cubi-tk/issues/303)) ([bd1414a](https://github.com/bihealth/cubi-tk/commit/bd1414a56ed854720b5ed0d926480e1851ffe1eb))
* refactored function didn't retrun anything ([#286](https://github.com/bihealth/cubi-tk/issues/286)) ([01c849b](https://github.com/bihealth/cubi-tk/commit/01c849b9e02b4b7a9cd922b5b0adb19fe0d62674))
* replaced deprecated logger with loguru ([#273](https://github.com/bihealth/cubi-tk/issues/273)) ([d065ae9](https://github.com/bihealth/cubi-tk/commit/d065ae9be57b6fc3fe308f5b493aed1e11a1cb8a))
* resolve merge issues from SodarIngestBase and deletion-request changes ([#300](https://github.com/bihealth/cubi-tk/issues/300)) ([80ab2b7](https://github.com/bihealth/cubi-tk/commit/80ab2b7f19e7cf6e0eda5a34a1158d2e41d987a2))
* retrun last found match col ([#292](https://github.com/bihealth/cubi-tk/issues/292)) ([1655670](https://github.com/bihealth/cubi-tk/commit/16556709a8b3dd899d551364392896f69a13d247))
* set assay uuid from lz ([#307](https://github.com/bihealth/cubi-tk/issues/307)) ([cf869dc](https://github.com/bihealth/cubi-tk/commit/cf869dc2b38a8afc85d69a5b4358b4e6bea357d4))
* switch sodar profile with different irods_env ([#284](https://github.com/bihealth/cubi-tk/issues/284)) ([cf73749](https://github.com/bihealth/cubi-tk/commit/cf73749b9f6e2cb0f3d9b53ef7c88b4556ce9ce1))

## [0.6.0](https://github.com/bihealth/cubi-tk/compare/v0.5.2...v0.6.0) (2025-02-24)


### Features

* Update sodar_cli & python-irods client for base functionality ([#263](https://github.com/bihealth/cubi-tk/issues/263)) ([3a6f946](https://github.com/bihealth/cubi-tk/commit/3a6f946dd7d03851f7faa9442988cefa41096124))


### Bug Fixes

* [[#256](https://github.com/bihealth/cubi-tk/issues/256)] adapt check_local for cancer and pull_sheets fix ([#264](https://github.com/bihealth/cubi-tk/issues/264)) ([6e83503](https://github.com/bihealth/cubi-tk/commit/6e83503d5c33d319edc25ecac4b6ac5ce373f0ea))
* [[#256](https://github.com/bihealth/cubi-tk/issues/256)] cancer and multiassay support pull_sheets.py ([#258](https://github.com/bihealth/cubi-tk/issues/258)) ([de33b6a](https://github.com/bihealth/cubi-tk/commit/de33b6aba132ae2d50baa0c5af9c39b1323512fc))
* [[#256](https://github.com/bihealth/cubi-tk/issues/256)] support for multiple studies and multiple assays in sodar_common.py ([#259](https://github.com/bihealth/cubi-tk/issues/259)) ([d9def70](https://github.com/bihealth/cubi-tk/commit/d9def70fff67756424a9b7b345184002a0e87eab))
* [256] adapt sodar pull-raw-data for cancer ([#265](https://github.com/bihealth/cubi-tk/issues/265)) ([7253ad8](https://github.com/bihealth/cubi-tk/commit/7253ad85792482269f21fa7df0068ee59d8d4866))
* destination pattern for ONT prefix ([#266](https://github.com/bihealth/cubi-tk/issues/266)) ([d5a8de5](https://github.com/bihealth/cubi-tk/commit/d5a8de55299490182a4bee1113e60d02cc983fae))

## [0.5.2](https://github.com/bihealth/cubi-tk/compare/v0.5.1...v0.5.2) (2025-02-03)


### Bug Fixes

* Fix SodarAPI implementation & add tests ([#252](https://github.com/bihealth/cubi-tk/issues/252)) ([109ba3f](https://github.com/bihealth/cubi-tk/commit/109ba3f89f6eeca5eaca59a635ab7ad7d59487c4))
* update-samplesheet execute test & fix for snappy-compatible ([#254](https://github.com/bihealth/cubi-tk/issues/254)) ([9d309a0](https://github.com/bihealth/cubi-tk/commit/9d309a09ac79ddba99edaa9ff8f49ac2c209d380))

## [0.5.1](https://github.com/bihealth/cubi-tk/compare/v0.5.0...v0.5.1) (2025-01-24)


### Release-As

* 0.5.1 ([4ad4f0f](https://github.com/bihealth/cubi-tk/commit/4ad4f0f747961ec54069737e9a0c32207e5265a0))


### Bug Fixes

* require current setuptools version in build-system, update docs ([#246](https://github.com/bihealth/cubi-tk/issues/246)) ([261944b](https://github.com/bihealth/cubi-tk/commit/261944b86ecc6560ca13a7a683217ac5feed11b7))

## [0.5.0](https://github.com/bihealth/cubi-tk/compare/v0.4.0...v0.5.0) (2025-01-15)


### ⚠ BREAKING CHANGES

* remove deprecated functionality of `cubi-tk archive`

### Features

* add `--filter-status` option for controlling which landing zones are listed ([#239](https://github.com/bihealth/cubi-tk/issues/239)) ([900b67a](https://github.com/bihealth/cubi-tk/commit/900b67a26130c8fccbcb338fcb666f03779b181f))
* add `cubi-tk sodar update-samplesheet` ([#240](https://github.com/bihealth/cubi-tk/issues/240)) ([581e650](https://github.com/bihealth/cubi-tk/commit/581e65090c6147330f8077bb7f73622434e20f45))
* Add common functions for interfacing with python-irodsclient ([#202](https://github.com/bihealth/cubi-tk/issues/202)) ([67e6e49](https://github.com/bihealth/cubi-tk/commit/67e6e492642d13d2561ff313e84849dd21a298cb))
* Add generic SODAR ingest command ([#199](https://github.com/bihealth/cubi-tk/issues/199)) ([8548c97](https://github.com/bihealth/cubi-tk/commit/8548c9763565901596b5d2675657b7940a6440a1))
* Add new command sodar landing-zone-validate ([#219](https://github.com/bihealth/cubi-tk/issues/219)) ([98e21c1](https://github.com/bihealth/cubi-tk/commit/98e21c199e25f4ca99bc5376de5bcc21da265129))
* Adding cubi-tk snappy itransfer_sv_calling ([#213](https://github.com/bihealth/cubi-tk/issues/213)) ([5a00e40](https://github.com/bihealth/cubi-tk/commit/5a00e40c9c12d02935326a24e0a0fda83329183d))
* allow varfish case resubmission ([#224](https://github.com/bihealth/cubi-tk/issues/224)) ([964bcac](https://github.com/bihealth/cubi-tk/commit/964bcacfe665818bcafc4466b2358011e13c0f89))
* irods download refactoring and new generic sodar downloader with preset for dragen data ([#226](https://github.com/bihealth/cubi-tk/issues/226) ) ([#227](https://github.com/bihealth/cubi-tk/issues/227)) ([3fc38af](https://github.com/bihealth/cubi-tk/commit/3fc38af8fc913f58845b1efae15dcabbfc75b919))
* presets for cubi-tk sodar ingest-fastq ([#232](https://github.com/bihealth/cubi-tk/issues/232) ) ([#235](https://github.com/bihealth/cubi-tk/issues/235)) ([8669118](https://github.com/bihealth/cubi-tk/commit/8669118d389ae038c0758cd9dabbfe58435878d5))
* Switching `cubi-tk sodar ingest-fastq` from icommands to irods_common ([#217](https://github.com/bihealth/cubi-tk/issues/217)) ([cd9a3b9](https://github.com/bihealth/cubi-tk/commit/cd9a3b9d6ccfa49ccf44e10152b17df30009b2a4))
* Update ingest-fastq, allow to match samples against assay table to determine collections names ([#198](https://github.com/bihealth/cubi-tk/issues/198)) ([#203](https://github.com/bihealth/cubi-tk/issues/203)) ([8b3662d](https://github.com/bihealth/cubi-tk/commit/8b3662daeacfa18a320a2168c2cdc5013213ad3c))


### Bug Fixes

* adapt to varfish-cli &gt;=0.6.2 syntax ([#221](https://github.com/bihealth/cubi-tk/issues/221)) ([9619609](https://github.com/bihealth/cubi-tk/commit/9619609483f1f0f753d19354ab8957692b7f7898))
* isa-tab add-ped now only modifies values on process nodes of ped-supplied samples [#207](https://github.com/bihealth/cubi-tk/issues/207) ([#233](https://github.com/bihealth/cubi-tk/issues/233)) ([88977d4](https://github.com/bihealth/cubi-tk/commit/88977d427731db93a0e91b37ddba74e19835a012))
* parsing snappy config for  with WGS projects ([#231](https://github.com/bihealth/cubi-tk/issues/231)) ([10592f3](https://github.com/bihealth/cubi-tk/commit/10592f3a37601d4598458cf543dbb38c6113b552))
* varfish cli 06x uses new import syntax ([#220](https://github.com/bihealth/cubi-tk/issues/220)) ([#222](https://github.com/bihealth/cubi-tk/issues/222)) ([cb10dbb](https://github.com/bihealth/cubi-tk/commit/cb10dbbcedd9e9d287428f14873420c5b6d4898c))
* varfish-upload subprocess submission ([#225](https://github.com/bihealth/cubi-tk/issues/225)) ([0f7f5b2](https://github.com/bihealth/cubi-tk/commit/0f7f5b2d46d0f955eb70faf8741cc44033fe63ce))


### Miscellaneous Chores

* remove  functionality ([133bc87](https://github.com/bihealth/cubi-tk/commit/133bc87d3b75a7beb6611f92084f683593d8cf0b))
* Update list of authors ([ae7525e](https://github.com/bihealth/cubi-tk/commit/ae7525ef6e69632b3c968c79f2d8b5899e888084))

## [0.4.0](https://www.github.com/bihealth/cubi-tk/compare/v0.3.0...v0.4.0) (2023-10-27)


### Features

* allow for existing files when using irods download ([#176](https://www.github.com/bihealth/cubi-tk/issues/176)) ([944a54c](https://www.github.com/bihealth/cubi-tk/commit/944a54c7ca5a33655eeffd4906d6aa0525550b0f))
* Fast irods querying ([#184](https://www.github.com/bihealth/cubi-tk/issues/184)) ([d614fcc](https://www.github.com/bihealth/cubi-tk/commit/d614fcc305cda3e629726a1136f7ebdb5915fb5b))
* Remove hardcoded snappy-pipeline steps ([#174](https://www.github.com/bihealth/cubi-tk/issues/174)) ([f171217](https://www.github.com/bihealth/cubi-tk/commit/f171217bdd40927ae31e4deae802b227845f2dc9))
* validate ISA-tabs after cookiecutter run ([#181](https://www.github.com/bihealth/cubi-tk/issues/181)) ([aeff82b](https://www.github.com/bihealth/cubi-tk/commit/aeff82b8d47c075f74fd6954c18d67b9b38f2be0))


### Bug Fixes

* actually match files recursively ([#180](https://www.github.com/bihealth/cubi-tk/issues/180)) ([e9c49b6](https://www.github.com/bihealth/cubi-tk/commit/e9c49b66a973a459b6d89b40469d9e1e0d31d97c))
* add space between value and unit symbol for file size formatting ([#166](https://www.github.com/bihealth/cubi-tk/issues/166)) ([79e85bf](https://www.github.com/bihealth/cubi-tk/commit/79e85bf4ae1686f5c5291254a836ce2a4580277a))
* adjust to change in ISA-tab template ([#186](https://www.github.com/bihealth/cubi-tk/issues/186)) ([#187](https://www.github.com/bihealth/cubi-tk/issues/187)) ([e4f46d4](https://www.github.com/bihealth/cubi-tk/commit/e4f46d47a5fcc22d3bb33f6c878c7194c25eb27c))
* allow mehari annotations ([#189](https://www.github.com/bihealth/cubi-tk/issues/189)) ([3939f83](https://www.github.com/bihealth/cubi-tk/commit/3939f83bf510d0dfdcfd698234c27f550342c20e))
* archive copy hangs ([#161](https://www.github.com/bihealth/cubi-tk/issues/161)) ([ae8bb92](https://www.github.com/bihealth/cubi-tk/commit/ae8bb92f6f4395f9a6ebd315d36870274197790e))
* broken symlinks ([#173](https://www.github.com/bihealth/cubi-tk/issues/173)) ([f76e480](https://www.github.com/bihealth/cubi-tk/commit/f76e480a9306b657467f76109f2e940ced32cbf0))
* fix to snappy varfish-upload ([#158](https://www.github.com/bihealth/cubi-tk/issues/158)) ([dfebe77](https://www.github.com/bihealth/cubi-tk/commit/dfebe7724e27522faa12dd18b7be495732efb12c))
* Fixes for changes introdueced when switching pull functions to query ([#188](https://www.github.com/bihealth/cubi-tk/issues/188)) ([5fcc369](https://www.github.com/bihealth/cubi-tk/commit/5fcc369be323ca802f5da6c1e0b545f2b9af1743))
* handling of <output_dir> input argument ([#163](https://www.github.com/bihealth/cubi-tk/issues/163)) ([bdf0e21](https://www.github.com/bihealth/cubi-tk/commit/bdf0e21f8c83e199877afcfef86163cd32cd0c9c))
* Unpin Mamba version in CI workflow ([#193](https://www.github.com/bihealth/cubi-tk/issues/193)) ([e5e5596](https://www.github.com/bihealth/cubi-tk/commit/e5e5596118ea19205cf1eb2afb1b3c9e92a2ef4e))
* Update ISA-tpl dependencies ([#195](https://www.github.com/bihealth/cubi-tk/issues/195)) ([67e1f18](https://www.github.com/bihealth/cubi-tk/commit/67e1f180445e5fbc1fbafd22578c257811210121))
* use increment, not sub-total, for tqdm.update ([#182](https://www.github.com/bihealth/cubi-tk/issues/182)) ([88672be](https://www.github.com/bihealth/cubi-tk/commit/88672be4132e241cff499ce590a3712b2fe30bb0))


### Documentation

* recommend newer python ([#171](https://www.github.com/bihealth/cubi-tk/issues/171)) ([ce41a34](https://www.github.com/bihealth/cubi-tk/commit/ce41a3463463fc27ccf1c240437bded4b16f46bb))
