package profile

// Base 是进程在运行时的配置
type Base struct {
	GoMaxProcs int8   `toml:"go_max_procs"` // 最大处理线程P的数量
	Mode       string `toml:"mode"`
}

// Mongo  用于初始化MongoDB的配置
type Mongo struct {
	Url string `toml:"url"`
}

// Redis 用于初始化Redis数据库的配置
//
type Redis struct {
	Endpoints string `toml:"endpoints"`
	Password  string `toml:"password"`
}

// Mysql 用于初始化Mysql数据库的配置
type Mysql struct {
	Url string `toml:"url"`
}

// Mysql 用于初始化Mysql数据库的配置
type Kafka struct {
	Brokers string `toml:"url"`
}

// Service 用于初始化服务的配置
// 如果
type Service struct {
	Host            string `toml:"host"`              // 服务监听
	PprofEnabled    bool   `toml:"pprof_enabled"`     // 启用PPROF
	PprofPathPrefix string `toml:"pprof_path_prefix"` // PPROF的路径前缀,
}

// Logger 日志配置
// 在几乎所有的服务或工具当中，这个配置项目都不应该缺席
type Logger struct {
	Format     string `toml:"format"` // 日志的格式
	Level      string `toml:"level"`
	TimeFormat string `toml:"time_format"`
}
