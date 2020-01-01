package controllers

type TestController struct {
	BaseController
}

func (t *TestController) Test() {
	param := t.Ctx.Input.Param(":param")
	t.SetData(param)
}
