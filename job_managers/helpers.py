def PrePostMethod(inputClass):
    mainRun = inputClass.run
    preFunc = inputClass.pre if "pre" in inputClass.__dict__ else None
    postFunc = inputClass.post if "post" in inputClass.__dict__ else None

    def new_run(self, *args, **kwargs):
        if preFunc:
            self.pre()
        mainRun(self, *args, **kwargs)
        if postFunc:
            self.post()

    inputClass.run = new_run
    return inputClass
