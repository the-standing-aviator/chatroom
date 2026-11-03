package main

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed web
var webFS embed.FS

// webHandler 返回静态前端资源（web/ 目录）。前端是单页应用，/ 即 index.html；
// 后续若把 js/css 拆成独立文件，FileServer 也会一并服务，无需改这里。
func webHandler() http.Handler {
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err) // 静态嵌入内容，运行时不会失败；出错是编译/打包 bug
	}
	return http.FileServer(http.FS(sub))
}
