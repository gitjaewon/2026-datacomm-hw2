// 실행 전에 공통 인자를 검사한다. 잘못된 설정으로 서버/Client가 대기하지 않도록 한다.
final class Arguments {
    private Arguments() {
    }

    static void requirePairs(String[] args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be --key value pairs.");
        }
    }

    static int positiveInt(String option, String value) {
        int number = Integer.parseInt(value);
        if (number <= 0) {
            throw new IllegalArgumentException(option + " must be positive.");
        }
        return number;
    }

    static void requireEndpoint(String host, int port) {
        if (host == null || host.isBlank() || port < 1 || port > 65535) {
            throw new IllegalArgumentException("A non-empty --host and --port (1..65535) are required.");
        }
    }
}
