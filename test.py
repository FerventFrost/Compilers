from app.etl.DataSoruces.Folder import FolderDS

if __name__ == "__main__":
    _operation = {'COLUMNS': '__all__'} 
    f= FolderDS()
    q = f.extract("C:\\Project\\results 3")
    d = FolderDS(q)
    q = d.transform(_operation)
    d.noMotion(q.get())
    d.to_df()