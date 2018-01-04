echo "download tarpaulin";
bash <(curl https://raw.githubusercontent.com/xd009642/tarpaulin/master/travis-install.sh);
echo "exec tarpaulin";
cargo tarpaulin --ciserver travis-ci --coveralls $TRAVIS_JOB_ID;
echo "out tarpaulin";
cargo tarpaulin --out Xml;
echo "upload tarpaulin";
bash <(curl -s https://codecov.io/bash);
