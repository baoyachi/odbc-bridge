echo "start exec isql test"
while ! nc -z localhost 5236;
do
  echo "wait for dm8_single";
  sleep 1;
done;

echo "dm8_single is ready!";
echo "start web service here";
env
~/.cargo/bin/cargo test -- --nocapture