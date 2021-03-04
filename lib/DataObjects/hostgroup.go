package dataobjects

type Hostgroup struct {
	Id    int
	Size  int
	Type  string
	Nodes []DataNode
}

func (hgw *Hostgroup) init(id int, hgType string, size int) *Hostgroup {
	hg := new(Hostgroup)
	hg.Id = id
	hg.Type = hgType
	hg.Size = size

	return hg
}
