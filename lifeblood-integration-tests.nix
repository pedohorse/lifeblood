{ pkgs }:
let
  firewall_settings = {
    allowedTCPPorts = [
      1384 1385 1386  # main control ports
    ];
    allowedUDPPorts = [
      34305  # broadcast
    ];
    allowedTCPPortRanges = [
      {  # message proxies
        from = 22182;
        to = 22200;
      }
    ];
  };
in pkgs.testers.runNixOSTest {
  name = "test of a test";
  nodes.server = { config, pkgs, ... }: {
    networking = {
      firewall = firewall_settings;
    };
    environment.systemPackages = with pkgs; [
      lifeblood
    ];
  };

  nodes.client = { pkgs, ... }: {
    networking = {
      # TODO: clients should have stricter settings
      firewall = firewall_settings;
    };
    environment.systemPackages = with pkgs; [
      lifeblood
    ];
  };

  testScript = ''
    import time
    import json

    def wait_for_json_data(machine, command, expected_data, timeout=30):
        for _ in range(max(1, int(timeout))):
            stdout = machine.succeed(command)
            queried_data = json.loads(stdout)
            if expected_data != queried_data:
                time.sleep(1)
                continue
            break
        print('raw query stdout:')
        print(stdout)
        assert queried_data == expected_data

    print('=====================')
    print('=== INIT MACHINES ===')
    print('=====================')


    server.wait_for_unit("default.target")
    client.wait_for_unit("default.target")

    server.succeed("ip addr show >&2")
    client.succeed("ip addr show >&2")

    server.succeed("lifeblood --loglevel DEBUG scheduler --broadcast-interval 3 >&2 &")
    client.succeed("lifeblood --loglevel DEBUG pool simple >&2 &")

    # first wait till connection can be established
    print(r'\/\/\/ ERRORS BELOW ARE EXPECTED \/\/\/ ')
    server.wait_until_succeeds("lifeblood_cli query workers")
    print(r'/\/\/\ ERRORS ABOVE ARE EXPECTED /\/\/\ ')

    print('================================')
    print('=== TRIVIAL STATE LOGIC TEST ===')
    print('================================')

    # then actually wait for data to get into correct state
    wait_for_json_data(server, "lifeblood_cli query --json workers", {"1": 1, "2": 1})  # str keys cuz json limitation

    node_id = server.succeed("lifeblood_cli create node null --name FOO")
    assert node_id[-1] != '\n'  # should return just value, no newline
    task_id = server.succeed(f"lifeblood_cli create task --node-id {node_id} --name footask --attributes qwe=asd foo=bar into=42 floato=4.2 boolo=True")
    assert task_id[-1] != '\n'  # should return just value, no newline

    # str keys cuz json limitation
    wait_for_json_data(
        server,
        f"lifeblood_cli query --json tasks --task-ids {task_id}",
        {str(task_id): {'state': 6, 'attributes': {'qwe': 'asd', 'foo': 'bar', 'into': 42, 'floato': 4.2, 'boolo': True}}},
    )

    print('=============================')
    print('=== BASIC INVOCATION TEST ===')
    print('=============================')

    node_id = server.succeed(''''lifeblood_cli create node python --name PYTHON1 --parameters process='schedule()' invoke='import time, lifeblood_connection;print("start");time.sleep(1);lifeblood_connection.set_attributes({"alice": task["alice"]+"_shmob", "into": task["into"]+1});print("finish")' '''')
    task_id = server.succeed(f"lifeblood_cli create task --node-id {node_id} --name pythontask --attributes alice=bob into=100")

    wait_for_json_data(
        server,
        f"lifeblood_cli query --json tasks --task-ids {task_id}",
        {str(task_id): {'state': 6, 'attributes': {'alice': 'bob_shmob', 'into': 101}}},
    )
  '';
}
