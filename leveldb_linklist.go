package utils

import (
	// "github.com/hyahm/golog"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	leveldbkey_link_start = []byte{1}
	leveldbkey_link_end   = []byte{2}

	leveldbkey_link_before = []byte{3}
	leveldbkey_link_body   = []byte{4}
	leveldbkey_link_next   = []byte{5}
)

/*
	查询此链表是否存在
*/
func (this *LevelDB) ExistLinkList(keyPre *[]byte) (bool, error) {
	_, err := this.Find(append(*keyPre, leveldbkey_link_start...))
	if err != nil {
		if err.Error() == leveldb.ErrNotFound.Error() {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

/*
	添加至头部
*/
func (this *LevelDB) AddToStart(keyPre *[]byte, orderId uint64, content *[]byte) error {
	// golog.Infof("AddToStart orderid:%d\n", orderId)

	//先查询记录是否存在
	orderIdBs := Uint64ToBytes(orderId)
	bodyKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_body)+len(orderIdBs))
	bodyKey = append(bodyKey, *keyPre...)
	bodyKey = append(bodyKey, leveldbkey_link_body...)
	bodyKey = append(bodyKey, orderIdBs...)
	_, err := this.Find(bodyKey)
	if err != nil {
		if err.Error() == leveldb.ErrNotFound.Error() {
		} else {
			// golog.Infof("AddToStart orderid:%d error:%s\n", orderId, err.Error())
			return err
		}
	} else {
		// golog.Infof("AddToStart orderid:%d 已经存在\n", orderId)
		return nil
	}

	exist := true
	startOrderIdBs, err := this.Find(append(*keyPre, leveldbkey_link_start...))
	if err != nil {
		if err.Error() == leveldb.ErrNotFound.Error() {
			exist = false
		} else {
			return err
		}
	}

	// orderIdBs := Uint64ToBytes(orderId)
	key_start := append(*keyPre, leveldbkey_link_start...)
	key_end := append(*keyPre, leveldbkey_link_end...)

	beforeKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_before)+len(orderIdBs))
	beforeKey = append(beforeKey, *keyPre...)
	beforeKey = append(beforeKey, leveldbkey_link_before...)
	beforeKey = append(beforeKey, orderIdBs...)

	// bodyKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_body)+len(orderIdBs))
	// bodyKey = append(bodyKey, *keyPre...)
	// bodyKey = append(bodyKey, leveldbkey_link_body...)
	// bodyKey = append(bodyKey, orderIdBs...)

	nextKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_next)+len(orderIdBs))
	nextKey = append(nextKey, *keyPre...)
	nextKey = append(nextKey, leveldbkey_link_next...)
	nextKey = append(nextKey, orderIdBs...)

	err = this.Save(key_start, &orderIdBs)
	if err != nil {
		return err
	}
	err = this.Save(bodyKey, content)
	if err != nil {
		return err
	}
	err = this.Save(beforeKey, nil)
	if err != nil {
		return err
	}
	if exist {
		//已经存在，则插入
		oldbeforeKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_before)+len(*startOrderIdBs))
		oldbeforeKey = append(oldbeforeKey, *keyPre...)
		oldbeforeKey = append(oldbeforeKey, leveldbkey_link_before...)
		oldbeforeKey = append(oldbeforeKey, *startOrderIdBs...)
		err = this.Save(nextKey, startOrderIdBs)
		if err != nil {
			return err
		}
		err = this.Save(oldbeforeKey, &orderIdBs)
		if err != nil {
			return err
		}
	} else {
		//不存在
		//
		err = this.Save(nextKey, nil)
		if err != nil {
			return err
		}

		err = this.Save(key_end, &orderIdBs)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
	添加至中间，按照orderId顺序来插入
*/
func (this *LevelDB) AddToBody(keyPre *[]byte, orderId uint64, content *[]byte) error {
	// golog.Infof("AddToBody orderid:%d\n", orderId)
	//先查询记录是否存在
	orderIdBs := Uint64ToBytes(orderId)
	bodyKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_body)+len(orderIdBs))
	bodyKey = append(bodyKey, *keyPre...)
	bodyKey = append(bodyKey, leveldbkey_link_body...)
	bodyKey = append(bodyKey, orderIdBs...)
	_, err := this.Find(bodyKey)
	if err != nil {
		if err.Error() == leveldb.ErrNotFound.Error() {
		} else {
			// golog.Infof("AddToBody orderid:%d error:%s\n", orderId, err.Error())
			return err
		}
	} else {
		// golog.Infof("AddToBody orderid:%d 已经存在\n", orderId)
		return nil
	}

	//先查最后一条记录
	exist := true
	endOrderIdBs, err := this.Find(append(*keyPre, leveldbkey_link_end...))
	if err != nil {
		if err.Error() == leveldb.ErrNotFound.Error() {
			exist = false
		} else {
			// golog.Infof("AddToBody orderid:%d error:%s\n", orderId, err.Error())
			return err
		}
	}
	if !exist {
		//没有记录
		return this.AddToStart(keyPre, orderId, content)
	}
	endOrderId := BytesToUint64(*endOrderIdBs)
	// golog.Infof("AddToBody orderid:%d equse:%d %d\n", orderId, orderId, endOrderId)
	if orderId > endOrderId {
		return this.AddToEnd(keyPre, orderId, content)
	}
	//再查询第一条记录
	startOrderIdBs, err := this.Find(append(*keyPre, leveldbkey_link_start...))
	if err != nil {
		// golog.Infof("AddToBody orderid:%d error:%s\n", orderId, err.Error())
		return err
	}
	startOrderId := BytesToUint64(*startOrderIdBs)
	if orderId < startOrderId {
		return this.AddToStart(keyPre, orderId, content)
	}
	if orderId == startOrderId || orderId == endOrderId {
		//重复的不作修改
		// fmt.Println("重复的不作修改")
		return nil
	}

	//判断头和尾哪个记录离插入的id比较近，以确定查询顺序
	directionKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_next)+len(*startOrderIdBs))
	directionKey = append(directionKey, *keyPre...)
	directionKey = append(directionKey, leveldbkey_link_next...)
	directionKey = append(directionKey, *startOrderIdBs...)
	directionOrderIdBs := startOrderIdBs
	left := false
	distanceStart := orderId - startOrderId
	distanceEnd := endOrderId - orderId
	if distanceEnd < distanceStart {
		left = true
		directionOrderIdBs = endOrderIdBs
		directionKey = make([]byte, 0, len(*keyPre)+len(leveldbkey_link_before)+len(*endOrderIdBs))
		directionKey = append(directionKey, *keyPre...)
		directionKey = append(directionKey, leveldbkey_link_before...)
		directionKey = append(directionKey, *endOrderIdBs...)
	}
	var tempOrderBs *[]byte
	var tempOrderId uint64
	for {
		// golog.Infof("AddToBody orderid:%d 查找位置:%d\n", orderId, tempOrderId)
		tempOrderBs, err = this.Find(directionKey)
		if err != nil {
			return err
		}
		if tempOrderBs == nil {
			// golog.Infof("AddToBody orderid:%d find key is nil!\n", orderId)
			return nil
		}
		tempOrderId = BytesToUint64(*tempOrderBs)
		if left {
			if tempOrderId < orderId {
				break
			}
			copy(directionKey[len(*keyPre):], leveldbkey_link_before)
		} else {
			if tempOrderId > orderId {
				break
			}
			copy(directionKey[len(*keyPre):], leveldbkey_link_next)
		}
		copy(directionKey[len(*keyPre)+len(leveldbkey_link_next):], *tempOrderBs)
		directionOrderIdBs = tempOrderBs
	}
	//找到位置，开始插入
	// key_start := append(*keyPre, leveldbkey_link_start...)
	// key_end := append(*keyPre, leveldbkey_link_end...)

	beforeKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_before)+len(orderIdBs))
	beforeKey = append(beforeKey, *keyPre...)
	beforeKey = append(beforeKey, leveldbkey_link_before...)
	beforeKey = append(beforeKey, orderIdBs...)

	nextKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_next)+len(orderIdBs))
	nextKey = append(nextKey, *keyPre...)
	nextKey = append(nextKey, leveldbkey_link_next...)
	nextKey = append(nextKey, orderIdBs...)

	err = this.Save(bodyKey, content)
	if err != nil {
		// golog.Infof("AddToBody orderid:%d error:%s\n", orderId, err.Error())
		return err
	}

	var leftOrderBs, rightOrderBs *[]byte
	if left {
		leftOrderBs = tempOrderBs
		rightOrderBs = directionOrderIdBs
	} else {
		leftOrderBs = directionOrderIdBs
		rightOrderBs = tempOrderBs
	}

	//插入到找到位置的右边
	leftKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_next)+len(*leftOrderBs))
	leftKey = append(leftKey, *keyPre...)
	leftKey = append(leftKey, leveldbkey_link_next...)
	leftKey = append(leftKey, *leftOrderBs...)

	err = this.Save(beforeKey, &orderIdBs)
	if err != nil {
		// golog.Infof("AddToBody orderid:%d error:%s\n", orderId, err.Error())
		return err
	}

	err = this.Save(beforeKey, leftOrderBs)
	if err != nil {
		// golog.Infof("AddToBody orderid:%d error:%s\n", orderId, err.Error())
		return err
	}
	err = this.Save(nextKey, rightOrderBs)
	if err != nil {
		// golog.Infof("AddToBody orderid:%d error:%s\n", orderId, err.Error())
		return err
	}

	rightKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_before)+len(*rightOrderBs))
	rightKey = append(rightKey, *keyPre...)
	rightKey = append(rightKey, leveldbkey_link_before...)
	rightKey = append(rightKey, *rightOrderBs...)

	err = this.Save(rightKey, &orderIdBs)
	if err != nil {
		// golog.Infof("AddToBody orderid:%d error:%s\n", orderId, err.Error())
		return err
	}
	// golog.Infof("AddToBody orderid:%d finish!\n", orderId)
	return nil
}

/*
	添加至尾部
*/
func (this *LevelDB) AddToEnd(keyPre *[]byte, orderId uint64, content *[]byte) error {

	//先查询记录是否存在
	orderIdBs := Uint64ToBytes(orderId)
	bodyKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_body)+len(orderIdBs))
	bodyKey = append(bodyKey, *keyPre...)
	bodyKey = append(bodyKey, leveldbkey_link_body...)
	bodyKey = append(bodyKey, orderIdBs...)
	// golog.Infof("AddToEnd orderid:%d key:%s\n", orderId, hex.EncodeToString(bodyKey))
	_, err := this.Find(bodyKey)
	if err != nil {
		if err.Error() == leveldb.ErrNotFound.Error() {
			// golog.Infof("AddToEnd orderid:%d not find\n", orderId)
		} else {
			// golog.Infof("AddToEnd orderid:%d error:%s\n", orderId, err.Error())
			return err
		}
	} else {
		// golog.Infof("AddToEnd orderid:%d 已经存在\n", orderId)
		return nil
	}

	exist := true
	endOrderIdBs, err := this.Find(append(*keyPre, leveldbkey_link_end...))
	if err != nil {
		if err.Error() == leveldb.ErrNotFound.Error() {
			exist = false
		} else {
			return err
		}
	}

	// orderIdBs := Uint64ToBytes(orderId)
	key_start := append(*keyPre, leveldbkey_link_start...)
	key_end := append(*keyPre, leveldbkey_link_end...)

	beforeKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_before)+len(orderIdBs))
	beforeKey = append(beforeKey, *keyPre...)
	beforeKey = append(beforeKey, leveldbkey_link_before...)
	beforeKey = append(beforeKey, orderIdBs...)

	// bodyKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_body)+len(orderIdBs))
	// bodyKey = append(bodyKey, *keyPre...)
	// bodyKey = append(bodyKey, leveldbkey_link_body...)
	// bodyKey = append(bodyKey, orderIdBs...)

	nextKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_next)+len(orderIdBs))
	nextKey = append(nextKey, *keyPre...)
	nextKey = append(nextKey, leveldbkey_link_next...)
	nextKey = append(nextKey, orderIdBs...)

	err = this.Save(key_end, &orderIdBs)
	if err != nil {
		return err
	}
	err = this.Save(bodyKey, content)
	if err != nil {
		return err
	}
	err = this.Save(nextKey, nil)
	if err != nil {
		return err
	}
	if exist {
		//已经存在，则插入
		oldnextKey := make([]byte, 0, len(*keyPre)+len(leveldbkey_link_next)+len(*endOrderIdBs))
		oldnextKey = append(oldnextKey, *keyPre...)
		oldnextKey = append(oldnextKey, leveldbkey_link_next...)
		oldnextKey = append(oldnextKey, *endOrderIdBs...)
		err = this.Save(beforeKey, endOrderIdBs)
		if err != nil {
			return err
		}
		err = this.Save(oldnextKey, &orderIdBs)
		if err != nil {
			return err
		}
	} else {
		//不存在
		//
		err = this.Save(beforeKey, nil)
		if err != nil {
			return err
		}

		err = this.Save(key_start, &orderIdBs)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
	删除至头部
*/
func (this *LevelDB) DelToStart(keyPre *[]byte, orderId uint64, content *[]byte) error {
	return nil
}

/*
	删除至中间
*/
func (this *LevelDB) DelToBody() error {
	return nil
}

/*
	删除至尾部
*/
func (this *LevelDB) DelToEnd() error {
	return nil
}

/*
	分页查询链表
	@linkName       *[]byte   链表名称
	@startOrderId   uint64    开始id
	@limit          uint64    总共查询多少条记录
	@isLeft         bool      排序查询方向，是否向左查询
*/
func (this *LevelDB) FindLinkList(linkName *[]byte, startOrderId uint64, limit uint64, isLeft bool) error {
	// key_end := append(*linkName, leveldbkey_link_end...)
	// if startOrderId == 0 {
	// 	//从最左边开始查询
	// 	key_start := append(*linkName, leveldbkey_link_start...)
	// }
	// if isLeft {
	// } else {
	// }
	return nil
}

/*
	查询一条记录
	@linkName       *[]byte   链表名称
	@orderId        uint64    要查询的id
*/
func (this *LevelDB) FindLinkByOrderId(linkName *[]byte, orderId uint64) (*[]byte, error) {
	orderIdBs := Uint64ToBytes(orderId)
	bodyKey := make([]byte, 0, len(*linkName)+len(leveldbkey_link_body)+len(orderIdBs))
	bodyKey = append(bodyKey, *linkName...)
	bodyKey = append(bodyKey, leveldbkey_link_body...)
	bodyKey = append(bodyKey, orderIdBs...)
	return this.Find(bodyKey)
}
