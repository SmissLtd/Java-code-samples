package net.bg.tandems.media.broadcast;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Transient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bg.tandems.domain.entity.BaseEntity;
import net.bg.tandems.domain.entity.Tandem;
import net.bg.tandems.domain.entity.Videoman;
import net.bg.tandems.domain.entity.settings.BroadcastSettings;
import net.bg.tandems.media.MediaUtils;

@Entity
@NamedQuery(name = "Playlist.findByType", query = "SELECT p FROM Playlist p WHERE p.type = :type ORDER BY p.id")
public class Playlist extends BaseEntity {
	private static final Logger LOG = LoggerFactory.getLogger(Playlist.class);

	private PlaylistType type;
	private List<AbstractPlaylistItem> items = new ArrayList<>();

	private int currentItemIndex = -1;
	private final List<Consumer<PlaylistChangedEvent>> changeListeners = new ArrayList<>();

	public Playlist() {
	}

	public Playlist(PlaylistType type) {
		this.type = type;
	}

	/**
	 * Adds set of files into playlist. All files should be of the same type (video or photo).
	 * @param files
	 * @param tandem
	 * @param videoman
	 */
	synchronized void addFiles(List<File> files, Tandem tandem, Videoman videoman, BroadcastSettings settings) {
		LOG.debug("Add files to playlist: " + files);

		List<AbstractPlaylistItem> newItems = createPlaylistItems(files, tandem, videoman, settings);

        int insertionPos = calculateNewMediaPosition(tandem, settings);

        if (insertionPos > -1) {
			items.addAll(insertionPos, newItems);
		} else {
            insertionPos = items.size();
            items.addAll(newItems);
        }

        if (insertionPos <= currentItemIndex) {
            currentItemIndex += newItems.size();
        }

        int finalInsertionPos = insertionPos;
        changeListeners.forEach(listener -> {
            final int[] i = {finalInsertionPos};
            newItems.forEach(item -> listener.accept(new PlaylistChangedEvent(PlaylistChangedEvent.EventType.ADDED, item, i[0]++)));
        });
	}

	public static List<AbstractPlaylistItem> createPlaylistItems(List<File> files, Tandem tandem, Videoman videoman, BroadcastSettings settings) {
		int repeatCount = settings.getPlaylistRepeatNewMedia();

		List<AbstractPlaylistItem> newItems = files.stream().filter(f -> MediaUtils.isVideo(f.getName())).map(v -> new VideoPlaylistItem(tandem, videoman, v,
				repeatCount)).collect(Collectors.toList());
		if (files.stream().anyMatch(f -> MediaUtils.isPhoto(f.getName()))) {
			double slideShowSpeedSecs = settings.getSlideShowSpeed();
			newItems.add(new PhotoPlaylistItem(tandem, videoman, files.stream().filter(f -> MediaUtils.isPhoto(f.getName())).collect(Collectors.toList()),
					repeatCount, slideShowSpeedSecs));
		}
		return newItems;
	}

	private int calculateNewMediaPosition(Tandem tandem, BroadcastSettings settings) {
		int insertionPos = settings.getPlaylistAddNewMedia().equals(BroadcastSettings.BroadcastSettingsNewMediaLocation.BEFORE) ?
				Math.max(currentItemIndex, 0) : Math.min(currentItemIndex + 1, items.size());

		if (settings.isPlaylistAddSameTandemAfter()) {
			int lastSameTandemItem = IntStream.range(0, items.size()).filter(i -> items.get(i).tandem.getId().equals(tandem.getId()))
					.reduce((a, b) -> b).orElse(-1);
			if (lastSameTandemItem > -1) {
				insertionPos = lastSameTandemItem + 1;
			}
		}

		return insertionPos;
	}
	
	public int indexOf(AbstractPlaylistItem item) {
		return items.indexOf(item);
	}

	synchronized public void remove(AbstractPlaylistItem item) {
		int i = items.indexOf(item);
		items.remove(item);
		if (i <= currentItemIndex) {
			currentItemIndex--;
		}

		changeListeners.forEach(listener -> listener.accept(new PlaylistChangedEvent(PlaylistChangedEvent.EventType.REMOVED, item, i)));
	}

	synchronized public void add(AbstractPlaylistItem item) {
		add(items.size(), item);
	}

	/**
     * @throws IndexOutOfBoundsException if the index is out of range
     *         ({@code index < 0 || index > size()})
     */
	synchronized public void add(int index, AbstractPlaylistItem item) {
		items.add(index, item);
		changeListeners.forEach(listener -> listener.accept(new PlaylistChangedEvent(PlaylistChangedEvent.EventType.ADDED, item, index)));
	}
	
	synchronized public void fireChanged(AbstractPlaylistItem item) {
		int index = items.indexOf(item);
		if (index != -1) {
			changeListeners.forEach(listener -> listener.accept(new PlaylistChangedEvent(PlaylistChangedEvent.EventType.CHANGED, item, index)));
		}
	}

	@Transient
	synchronized public AbstractPlaylistItem getCurrentItem() {
		AbstractPlaylistItem currentItem = null;
		if (currentItemIndex > -1 && currentItemIndex < items.size()) {
			currentItem = items.get(currentItemIndex);
		}
		return currentItem;
	}

	synchronized public AbstractPlaylistItem nextItem() {
		currentItemIndex++;
		if (currentItemIndex >= items.size()) {
			resetPlayback();
		}
		return getCurrentItem();
	}

	synchronized public void resetPlayback() {
		currentItemIndex = -1;
	}

	@Transient
	synchronized public AbstractPlaylistItem getItem(int index) {
		return items.get(index);
	}

	@OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
	public List<AbstractPlaylistItem> getItems() {
		return items;
	}

	synchronized public void setItems(List<AbstractPlaylistItem> items) {
		this.items = items;
	}

	public void addChangeListener(Consumer<PlaylistChangedEvent> listener) {
		changeListeners.add(listener);
	}

	@Enumerated(EnumType.STRING)
	public PlaylistType getType() {
		return type;
	}

	public void setType(PlaylistType type) {
		this.type = type;
	}

	public int getCurrentItemIndex() {
		return currentItemIndex;
	}

	public void setCurrentItemIndex(int currentItemIndex) {
		this.currentItemIndex = currentItemIndex;
	}
}

